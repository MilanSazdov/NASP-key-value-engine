#include "LSMManager.h"
#include <iostream>
#include <algorithm>
#include <cmath>
namespace fs = std::filesystem;

LSMManager::LSMManager(SSTManager* sstManager, Config* config)
    : config(config) {
    std::cout << "[LSM] Stateless LSM Manager initialized." << std::endl;
}

// OVO MOZDA NE TREBA
    /*
LSMManager::LSMManager(std::unique_ptr<CompactionStrategy> strategy, int max_levels, SSTManager& sstRef)
    : base_directory_(Config::data_directory),
	manifest_path_(Config::data_directory + "/MANIFEST"),
    strategy_(std::move(strategy)),
    max_levels_(max_levels),
    sstManager_(sstRef),
    stop_worker_(false),
    compaction_needed_(false) {
    levels_.resize(max_levels_);
}*/

LSMManager::~LSMManager() {
}

void LSMManager::triggerCompactionCheck() {
	// TODO: Implementirati logiku za pokretanje kompakcije

    /*std::cout << "[LSM] Compaction check triggered." << std::endl;
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
    */
}
