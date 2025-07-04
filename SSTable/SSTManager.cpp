#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include "SSTManager.h"
#include "SSTable.h"
#include "BloomFilter.h"

namespace fs = std::filesystem;

using ull = unsigned long long;

SSTManager::SSTManager() : directory_(Config::data_directory) {
    cout << Config::data_directory << endl;
}

// pomocna funkcija za pronalazenje sledeceg ID-a
int SSTManager::findNextIndex(const std::string& levelDir) const {
    int max_id = -1;
    if (!fs::exists(levelDir) || !fs::is_directory(levelDir)) {
        return 0; // prvi fajl ce biti sa ID-em 0
    }

    for (const auto& entry : fs::directory_iterator(levelDir)) {
        std::string filename = entry.path().filename().string();
        if (filename.rfind("sstable_", 0) == 0 && entry.path().extension() == ".sst") {
            try {
                // Primer: iz "sstable_5.sst" izvuci "5"
                std::string id_str = filename.substr(8, filename.find('.') - 8);
                int id = std::stoi(id_str);
                if (id > max_id) {
                    max_id = id;
                }
            }
            catch (const std::exception& e) {
                // Ignorisi fajlove sa nevalidnim formatom imena
                continue;
            }
        }
    }
    return max_id + 1;
}

optional<string> SSTManager::get_from_level(const std::string& key, bool& deleted, int level) const
{
    deleted = false;
    string current_directory = directory_ + "level_" + to_string(level);
    cout << "current_directory == " << current_directory << endl;
    
    if (!fs::is_directory(current_directory)) {
        cout << "Error get_from_level " << current_directory << " is not valid directory\n";
        throw exception();
    }
    if (fs::exists(current_directory)) return nullopt;

    std::vector<Record> matches;
    
    for (const auto& entry : fs::directory_iterator(directory_))
    {
        if (entry.is_regular_file()) {
            std::string filename = entry.path().filename().string();
            cout << filename << endl;
            // Prolazimo kroz sve filter fajlove
            if (filename.rfind("filter", 0) == 0) {

                // Citamo fajl
                std::size_t fileSize = fs::file_size(directory_+filename);
                
                std::vector<byte> fileData(fileSize);
                if (fileSize == 0) {
                    cout << "File : " << filename << ".size() == 0\n";
                    continue;
                }
                std::ifstream file(directory_ + filename, std::ios::binary);
                if (!file) {
                    throw std::runtime_error("[SSTManager] Ne mogu da otvorim fajl: " + filename);
                }

                file.read(reinterpret_cast<char*>(fileData.data()), fileSize);


                BloomFilter bloom = BloomFilter::deserialize(fileData);

                if (bloom.possiblyContains(key)) {
                    // Gledamo koji je broj
                    std::size_t underscorePos = filename.find('_');
                    std::size_t dotPos = filename.find('.', underscorePos);
                    if (underscorePos == std::string::npos || dotPos == std::string::npos) {
                        cerr << ("[SSTManager] Ne validan format: " + filename);
                        continue;
                    }

                    std::string numStr = filename.substr(underscorePos + 1, dotPos - underscorePos - 1);
                    std::string ending = numStr + ".sst";

                    SSTable sst((directory_ + "sstable_" + ending),
                        (directory_ + "index_" + ending),
                        (directory_ + "filter_" + ending),
                        (directory_ + "summary_" + ending),
                        (directory_ + "meta_" + ending));

                    //Sve recorde sa odgovarajucim key-em stavljamo u vektor
                    std::vector<Record> found = sst.get(key);
                    matches.insert(matches.end(), found.begin(), found.end());
                }
            }
        }
    }

    Record rMax;
    ull tsMax = 0;

    for (Record r : matches)
    {
        if (r.timestamp > tsMax)
        {
            tsMax = r.timestamp;
            rMax = r;
        }
    }


    // not found. Return nullopt
    if (tsMax == 0) nullopt;

    // found. Record is deleted. Return nullopt + DELETED = TRUE
    else if (static_cast<int>(rMax.tombstone) == 1)
    {
        deleted = true;
        return nullopt;
    }

    // found. Return value 
    return rMax.value;
}

// pretrazuje sstable po nivoima. Kad naide na nekom nivou na kljuc, to vraca. Kad prodje sve nivoe, ne postoji kljuc, vraca nullopt
optional<string> SSTManager::get(const std::string& key) const {
    bool deleted;
    for (int i = 0; i <= Config::max_levels; i++) {
        auto ret = get_from_level(key, deleted, i);
        // record doesnt exists BECAUSE IT IS DELETED
        if (deleted) return nullopt;
        // record exists
        else if(ret != nullopt) return ret.value();
    }
    //record doesnt exists because IT WASNT PUT
    return nullopt;
}

SSTableMetadata SSTManager::write(std::vector<Record> sortedRecords, int level) const {
    if (sortedRecords.empty()) {
        throw std::runtime_error("[SSTManager] Cannot write an SSTable with zero records.");
    }

    std::string levelDir = directory_ + "/level_" + std::to_string(level);

    // Osiguraj da direktorijum za nivo postoji
    if (!fs::exists(levelDir)) {
        fs::create_directories(levelDir);
    }

    int fileId = findNextIndex(levelDir);
    std::string numStr = std::to_string(fileId);
    std::string ending = "_" + numStr + ".sst"; // Koristimo _ da odvojimo ime i broj

    // Kreiraj putanje za sve komponente SSTable-a
    std::string data_path = levelDir + "/sstable" + ending;
    std::string index_path = levelDir + "/index" + ending;
    std::string filter_path = levelDir + "/filter" + ending;
    std::string summary_path = levelDir + "/summary" + ending;
    std::string meta_path = levelDir + "/meta" + ending;

    // Instanciraj SSTable objekat koji ce obaviti upisivanje
    SSTable sst(data_path, index_path, filter_path, summary_path, meta_path);

    // Ova metoda vrsi "tezak posao" upisivanja svih 5 fajlova.
    sst.build(sortedRecords);

    // Nakon sto su fajlovi upisani, kreiramo i popunjavamo metapodatke.
    SSTableMetadata meta;
    meta.level = level;
    meta.file_id = fileId;
    meta.min_key = sortedRecords.front().key; // Vektor je vec sortiran
    meta.max_key = sortedRecords.back().key;  // Vektor je vec sortiran

    // Sracunaj ukupnu velicinu fajla sabiranjem velicina svih komponenti
    uint64_t total_size = 0;
    try {
        total_size += fs::file_size(data_path);
        total_size += fs::file_size(index_path);
        total_size += fs::file_size(summary_path);
        total_size += fs::file_size(filter_path);
        total_size += fs::file_size(meta_path);
    }
    catch (const fs::filesystem_error& e) {
        std::cerr << "Error getting file size for SSTable " << fileId << ": " << e.what() << std::endl;
        // U slučaju greske, postavi velicinu na 0
        total_size = 0;
    }
    meta.file_size = total_size;

    // Sacuvaj pune putanje do fajlova u metapodacima
    meta.data_path = data_path;
    meta.index_path = index_path;
    meta.summary_path = summary_path;
    meta.filter_path = filter_path;
    meta.meta_path = meta_path;

    std::cout << "[SSTManager] Successfully wrote SSTable " << fileId << " to level " << level
        << " (Total Size: " << total_size << " bytes)." << std::endl;

    return meta;
}