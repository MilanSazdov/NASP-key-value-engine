#include "LSMManager.h"
#include <iostream>
#include <algorithm>
#include <cmath>

LSMManager::LSMManager(SSTManager* sstManager, Config* config)
    : sstManager(sstManager), config(config) {
    std::cout << "[LSM] Stateless LSM Manager initialized." << std::endl;
#include "LSMManager.h"
#include "SSTableIterator.h"
#include "MergeIterator.h"

namespace fs = std::filesystem;
// OVO MOZDA NE TREBA
LSMManager::LSMManager(std::unique_ptr<CompactionStrategy> strategy, int max_levels, SSTManager& sstRef)
    : base_directory_(Config::data_directory),
	manifest_path_(Config::data_directory + "/MANIFEST"),
    strategy_(std::move(strategy)),
    max_levels_(max_levels),
    sstManager_(sstRef),
    stop_worker_(false),
    compaction_needed_(false) {
    levels_.resize(max_levels_);
}

LSMManager::~LSMManager() {}

void LSMManager::triggerCompactionCheck() {
    std::cout << "[LSM] Compaction check triggered." << std::endl;
    // Petlja ide od najnižeg ka najvišem nivou.
    for (int level = 0; level < config->max_levels - 1; ++level) {
        // 1. UVEK ČITAJ TRENUTNO STANJE SA DISKA
        std::vector<SSTableMetadata> tablesOnLevel = sstManager->findTablesForLevel(level);

        bool needsCompaction = false;
        long long threshold = 0;

        if (config->compaction_strategy == "SizeTiered") {
            threshold = config->max_number_of_sstable_on_level;
            if (tablesOnLevel.size() >= threshold) {
                needsCompaction = true;
            }
        }
        else if (config->compaction_strategy == "Leveled") {
            if (level == 0) {
                threshold = config->l0_compaction_trigger;
            }
            else {
                // Izračunaj prag za trenutni nivo
                threshold = config->l0_compaction_trigger;
                for (int i = 1; i <= level; ++i) {
                    threshold *= config->level_size_multiplier;
                }
            }
            if (tablesOnLevel.size() >= threshold) {
                needsCompaction = true;
            }
        }

        if (needsCompaction) {
            std::cout << "[LSM] Compaction needed on level " << level
                << ". Current tables: " << tablesOnLevel.size()
                << ", Threshold: " << threshold << std::endl;

            // Pokreni kompakciju za ovaj nivo
            if (config->compaction_strategy == "SizeTiered") {
                sizeTieredCompaction(level, tablesOnLevel);
            }
            else {
                leveledCompaction(level, tablesOnLevel);
            }
            // Petlja se prirodno nastavlja na sledeći nivo, koji će sada imati nove tabele.
            // Ovo rešava problem lančane kompakcije.
        }
        else {
            // Ako na nekom nivou nema potrebe za kompakcijom, možemo prekinuti proveru
            // jer ni viši nivoi sigurno neće biti promenjeni.
            // std::cout << "[LSM] No compaction needed on level " << level << ". Stopping check." << std::endl;
            // break;  // Opciono: za malu optimizaciju.
        }
    }
}

void LSMManager::sizeTieredCompaction(int level, const std::vector<SSTableMetadata>& tablesOnLevel) {
    std::cout << "[LSM] Starting Size-Tiered compaction for " << tablesOnLevel.size()
        << " tables from level " << level << " to level " << level + 1 << "." << std::endl;

    // Spajamo SVE tabele sa ovog nivoa
    auto newMetas = sstManager->compactTables(tablesOnLevel, level + 1);

    if (!newMetas.empty()) {
        // Brišemo sve stare tabele koje su upravo spojene
        for (const auto& oldMeta : tablesOnLevel) {
            sstManager->deleteSSTable(oldMeta);
        }
        std::cout << "[LSM] Size-Tiered compaction finished for level " << level << "." << std::endl;
    }
    else {
        std::cerr << "[LSM] ERROR: Size-Tiered compaction failed on level " << level << std::endl;
    }
}

void LSMManager::leveledCompaction(int level, const std::vector<SSTableMetadata>& tablesOnLevel) {
    std::cout << "[LSM] Starting Leveled compaction for level " << level << "." << std::endl;

    // 1. Izaberi najstariju tabelu sa nivoa L za spajanje
    SSTableMetadata sourceTable = tablesOnLevel.front(); // Pretpostavljamo da su sortirane po starosti

    // 2. Pronađi sve tabele na sledećem nivou (L+1) koje se preklapaju sa njom
    std::vector<SSTableMetadata> nextLevelTables = sstManager->findTablesForLevel(level + 1);
    std::vector<SSTableMetadata> tablesToCompact;
    tablesToCompact.push_back(sourceTable); // Uvek uključujemo izvornu tabelu

    for (const auto& nextLevelTable : nextLevelTables) {
        bool overlaps = (sourceTable.minKey <= nextLevelTable.maxKey) && (sourceTable.maxKey >= nextLevelTable.minKey);
        if (overlaps) {
            tablesToCompact.push_back(nextLevelTable);
        }
    }

    std::cout << "[LSM] Compacting 1 table from L" << level << " with "
        << tablesToCompact.size() - 1 << " overlapping tables from L" << level + 1 << std::endl;

    // 3. Pozovi sstManager da spoji izabrane tabele
    auto newMetas = sstManager->compactTables(tablesToCompact, level + 1);

    if (!newMetas.empty()) {
        // 4. Obriši sve stare tabele koje su učestvovale u kompakciji
        for (const auto& meta : tablesToCompact) {
            sstManager->deleteSSTable(meta);
        }
        std::cout << "[LSM] Leveled compaction finished for level " << level << "." << std::endl;
    }
    else {
        std::cerr << "[LSM] ERROR: Leveled compaction failed on level " << level << std::endl;
    }
}