#pragma once
#include "../SSTable/SSTManager.h"
#include "../Config/Config.h"

class LSMManager {
public:
    LSMManager(SSTManager* sstManager, Config* config);
    ~LSMManager();

    // Glavna funkcija koja se poziva nakon flush-a iz Memtable-a.
    // Pokreće proveru i, ako je potrebno, lančanu kompakciju kroz sve nivoe.
    void triggerCompactionCheck();

private:
    SSTManager* sstManager;
    Config* config;

    // Specifična logika za Size-Tiered strategiju.
    void sizeTieredCompaction(int level, const std::vector<SSTableMetadata>& tablesOnLevel);

    // Specifična logika za Leveled strategiju.
    void leveledCompaction(int level, const std::vector<SSTableMetadata>& tablesOnLevel);
};