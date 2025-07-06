
#include <iostream>
#include <algorithm>
#include <filesystem>
#include <sstream>

#include "LSMManager.h"
#include "SSTableIterator.h"
#include "MergeIterator.h"

namespace fs = std::filesystem;

LSMManager::LSMManager(std::unique_ptr<CompactionStrategy> strategy, int max_levels, Block_manager& bm)
    : base_directory_(Config::data_directory),
	manifest_path_(Config::data_directory + "/MANIFEST"),
    strategy_(std::move(strategy)),
    max_levels_(max_levels),
    sstManager_(bm),
    stop_worker_(false),
    compaction_needed_(false) {
    levels_.resize(max_levels_);
}

LSMManager::~LSMManager() {
    stop_worker_ = true;
    compaction_needed_ = true; // Postavi na true da se nit sigurno probudi
    cv_.notify_one();          // Signaliziraj niti da se probudi i zavrsi
    if (compaction_thread_.joinable()) {
        compaction_thread_.join();
    }
}

std::string LSMManager::getLevelPath(int level) const {
    return base_directory_ + "/level_" + std::to_string(level);
}

void LSMManager::initialize() {
    for (int i = 0; i < max_levels_; ++i) {
        fs::create_directories(getLevelPath(i));
    }
    
	// Na pocetku, ucitavamo stanje iz MANIFEST fajla ako postoji
	load_state_from_manifest();

    // Pokreni pozadinsku nit
    compaction_thread_ = std::thread(&LSMManager::compaction_worker, this);
}

void LSMManager::load_state_from_manifest() {
    std::ifstream manifest_file(manifest_path_);
    if (!manifest_file.is_open()) {
        std::cout << "[LSMManager] MANIFEST file not found. Starting with a fresh state." << std::endl;
        return;
    }

    std::string line;
    int line_num = 0;
    while (std::getline(manifest_file, line)) {
        line_num++;
        std::stringstream ss(line);
        SSTableMetadata meta;

        // Format: level file_id min_key max_key file_size data_path index_path summary_path filter_path meta_path
        ss >> meta.level >> meta.file_id >> meta.min_key >> meta.max_key >> meta.file_size
            >> meta.data_path >> meta.index_path >> meta.summary_path >> meta.filter_path >> meta.meta_path;

        if (ss.fail()) {
            std::cerr << "[LSMManager] WARNING: Failed to parse line " << line_num << " in MANIFEST. Skipping." << std::endl;
            continue;
        }

        if (meta.level >= 0 && meta.level < max_levels_) {
            levels_[meta.level].push_back(meta);
        }
    }

    // sortiraj ucitane nivoe (LCS zahteva da nivoi > 0 budu sortirani po kljucu)
    for (int i = 1; i < max_levels_; ++i) {
        std::sort(levels_[i].begin(), levels_[i].end(),
            [](const SSTableMetadata& a, const SSTableMetadata& b) { return a.min_key < b.min_key; });
    }

    std::cout << "[LSMManager] Successfully loaded state from MANIFEST." << std::endl;
}

void LSMManager::save_state_to_manifest() const {
    std::string temp_manifest_path = manifest_path_ + ".tmp";
    std::ofstream temp_manifest_file(temp_manifest_path);

    if (!temp_manifest_file.is_open()) {
        std::cerr << "[LSMManager] ERROR: Could not open temporary MANIFEST file for writing." << std::endl;
        return;
    }

    // prodji kroz sve nivoe i upisi metapodatke svakog SSTable-a
    for (const auto& level_vec : levels_) {
        for (const auto& meta : level_vec) {
            temp_manifest_file << meta.level << " " << meta.file_id << " "
                << meta.min_key << " " << meta.max_key << " "
                << meta.file_size << " " << meta.data_path << " "
                << meta.index_path << " " << meta.summary_path << " "
                << meta.filter_path << " " << meta.meta_path << "\n";
        }
    }

    temp_manifest_file.close();

    // atomski preimenuj .tmp fajl u finalni MANIFEST fajl
    try {
        fs::rename(temp_manifest_path, manifest_path_);
    }
    catch (const fs::filesystem_error& e) {
        std::cerr << "[LSMManager] ERROR: Failed to rename temporary MANIFEST: " << e.what() << std::endl;
    }
}

void LSMManager::flushToLevel0(std::vector<Record>& records) {
    if (records.empty()) return;

    // Pretpostavka: sstManager_.write sada vraća SSTableMetadata
    // OVO MORATE IMPLEMENTIRATI U VAŠEM SSTManager-u
    SSTableMetadata new_sst_meta = sstManager_.write(records, 0);

    {
        std::lock_guard<std::mutex> lock(mtx_);
        levels_[0].push_back(new_sst_meta);
		save_state_to_manifest(); // Azuriraj MANIFEST nakon svakog flush-a
    }

    // Signaliziraj pozadinskoj niti da ima potencijalnog posla
    compaction_needed_ = true;
    cv_.notify_one();
}

void LSMManager::compaction_worker() {
    while (!stop_worker_) {
        std::unique_lock<std::mutex> lock(mtx_);
        // nit "spava" dok je ne probudi signal (iz flushToLevel0 ili nakon prethodne kompakcije)
        cv_.wait(lock, [this] { return compaction_needed_.load() || stop_worker_.load(); });

        if (stop_worker_) break; // proveri da li treba izaci

        compaction_needed_ = false; // resetuj signal

        // kopiraj trenutno stanje da bi mogao da otkljucas mutex dok strategija radi
        auto current_levels_snapshot = levels_;
        lock.unlock();

        // Pozovi strategiju da vidi da li ima posla
        if (auto plan_opt = strategy_->pickCompaction(current_levels_snapshot)) {
            std::cout << "[LSM WORKER] Triggering compaction...\n";
            do_compaction(*plan_opt);

            // Nakon jedne kompakcije, moguce je da je potrebna nova (kaskadiranje) => zato odmah signaliziramo ponovnu proveru
            compaction_needed_ = true;
            cv_.notify_one();
        }
    }
}

void LSMManager::do_compaction(const CompactionPlan& plan) {
    std::cout << "[LSM WORKER] Compacting "
        << plan.inputs_level_1.size() + plan.inputs_level_2.size()
        << " files to level " << plan.output_level << std::endl;

    std::vector<std::unique_ptr<SSTableIterator>> iterators;
    std::vector<SSTableMetadata> all_input_files = plan.inputs_level_1;
    all_input_files.insert(all_input_files.end(), plan.inputs_level_2.begin(), plan.inputs_level_2.end());

    if (all_input_files.empty()) {
        std::cout << "[LSM WORKER] Compaction plan has no input files. Skipping.\n";
        return;
    }

    // Sortiraj ulazne fajlove od najnovijih ka najstarijim (po ID-u)
    // Ovo je kljucno da MergeIterator ispravno resi duplikate
    std::sort(all_input_files.begin(), all_input_files.end(),
        [](const SSTableMetadata& a, const SSTableMetadata& b) {
            return a.file_id > b.file_id;
        });

    for (const auto& meta : all_input_files) {
        // Pretpostavka: Config::block_size je dostupan
        iterators.push_back(std::make_unique<SSTableIterator>(meta, 4096 /* Config::block_size */));
    }

    MergeIterator merge_iterator(std::move(iterators));

    // izlazna logika: pisi u nove SSTable fajlove.
    std::vector<Record> records_for_new_sst;
    std::vector<SSTableMetadata> new_sst_metadatas;
    uint64_t current_sst_size = 0;
    // Ciljana velicina fajla (za LCS). Treba da dodje iz konfiguracije.
    uint64_t target_file_size = 2 * 1024 * 1024; // Primer: 2MB

    while (merge_iterator.hasNext()) {
        Record r = merge_iterator.next();
        records_for_new_sst.push_back(r);
        current_sst_size += r.key_size + r.value_size + 30; // gruba aproksimacija velicine zapisa

        // Ako je izlazni fajl dostigao ciljanu velicinu (relevantno za LCS) => upisi ga i zapocni novi
        if (current_sst_size >= target_file_size) {
            new_sst_metadatas.push_back(sstManager_.write(records_for_new_sst, plan.output_level));
            records_for_new_sst.clear();
            current_sst_size = 0;
        }
    }

    // Ako je ostalo zapisa, upisi poslednji fajl
    if (!records_for_new_sst.empty()) {
        new_sst_metadatas.push_back(sstManager_.write(records_for_new_sst, plan.output_level));
    }

    // --- Atomsko azuriranje stanja ---
    {
        std::lock_guard<std::mutex> lock(mtx_);

        // 1. Obrisi stare fajlove iz memorijskog stanja
        for (const auto& meta_to_remove : all_input_files) {
            auto& level_vec = levels_[meta_to_remove.level];
            level_vec.erase(std::remove(level_vec.begin(), level_vec.end(), meta_to_remove), level_vec.end());
        }

        // 2. Dodaj nove fajlove u stanje
        auto& output_level_vec = levels_[plan.output_level];
        output_level_vec.insert(output_level_vec.end(), new_sst_metadatas.begin(), new_sst_metadatas.end());

        // 3. Za LCS, vazno je da nivoi > 0 budu sortirani po kljucu radi brze pretrage
        if (plan.output_level > 0) {
            std::sort(output_level_vec.begin(), output_level_vec.end(),
                [](const SSTableMetadata& a, const SSTableMetadata& b) { return a.min_key < b.min_key; });
        }

        // TODO: Pozvati save_state_to_manifest()
    }

    // 4. Tek nakon uspesnog azuriranja stanja, fizicki obrisi stare fajlove sa diska
    for (const auto& meta : all_input_files) {
        fs::remove(meta.data_path);
        fs::remove(meta.index_path);
        fs::remove(meta.summary_path);
        fs::remove(meta.filter_path);
        fs::remove(meta.meta_path);
    }

    std::cout << "[LSM WORKER] Compaction finished.\n";
}