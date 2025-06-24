#include "LSMManager.h"
#include <iostream>
#include <algorithm>
#include <SSTable.h>

namespace fs = std::filesystem;

LSMManager::LSMManager(const std::string& base_directory, int max_level, int compaction_threshold)
    : base_directory_(base_directory),
    max_level_(max_level),
    compaction_threshold_(compaction_threshold),
    sstManager_(base_directory) {
}

void LSMManager::initialize() {
    for (int i = 0; i <= max_level_; ++i) {
        fs::create_directories(base_directory_ + "/level_" + std::to_string(i));
    }
}

std::string LSMManager::getLevelPath(int level) const {
    return base_directory_ + "/level_" + std::to_string(level);
}


void LSMManager::flushToLevel0(std::vector<Record>& records) {
    sstManager_.write(records, 0); // upisujemo u level 0
}

void LSMManager::checkAndTriggerCompaction() {
    std::string level0Path = getLevelPath(0);
    int sstableCount = 0;

    for (const auto& entry : fs::directory_iterator(level0Path)) {
        std::string filename = entry.path().filename().string();
        if (filename.rfind("sstable_", 0) == 0 && entry.path().extension() == ".sst") {
            ++sstableCount;
        }
    }

    if (sstableCount >= compaction_threshold_) {
        std::cout << "[LSMManager] Triggering compaction on level 0\n";
        compactLevel(0);
    }
}

void LSMManager::compactLevel(int level) {
    if (level >= max_level_) {
        std::cout << "[LSMManager] Maximum level reached. Cannot compact further.\n";
        return;
    }

    std::string levelDir = getLevelPath(level);
    std::vector<Record> allRecords;

    // Skupi sve SSTable fajlove iz datog nivoa
    for (const auto& entry : fs::directory_iterator(levelDir)) {
        std::string filename = entry.path().filename().string();
        if (filename.rfind("sstable_", 0) == 0 && entry.path().extension() == ".sst") {
            std::string suffix = filename.substr(8); // "3.sst"
            std::string number = suffix.substr(0, suffix.find('.'));

            SSTable sst(
                levelDir + "/sstable_" + number + ".sst",
                levelDir + "/index_" + number + ".sst",
                levelDir + "/filter_" + number + ".sst",
                levelDir + "/summary_" + number + ".sst",
                levelDir + "/meta_" + number + ".sst"
            );

            std::vector<Record> records = sst.get("");
            allRecords.insert(allRecords.end(), records.begin(), records.end());

            // obrisi sve fajlove
            fs::remove(levelDir + "/sstable_" + number + ".sst");
            fs::remove(levelDir + "/index_" + number + ".sst");
            fs::remove(levelDir + "/filter_" + number + ".sst");
            fs::remove(levelDir + "/summary_" + number + ".sst");
            fs::remove(levelDir + "/meta_" + number + ".sst");
        }
    }

    // Sortiraj po kljucu i vremenu
    std::sort(allRecords.begin(), allRecords.end(), [](const Record& a, const Record& b) {
        return a.key < b.key || (a.key == b.key && a.timestamp > b.timestamp);
        });

    // Ukloni duplikate i tombstone-ovane zapise
    std::vector<Record> filtered;
    std::string lastKey = "";

    for (const auto& rec : allRecords) {
        if (rec.key != lastKey) {
            if (static_cast<int>(rec.tombstone) != 1) {
                filtered.push_back(rec);
            }
            lastKey = rec.key;
        }
    }

    // Zapisi u sledeci nivo
    sstManager_.write(filtered, level + 1);
}
