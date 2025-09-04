#pragma once
#include "../Config/Config.h"

#include <string>
#include <vector>
#include <filesystem>
#include <memory>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <atomic>

#include "../SSTable/SSTManager.h"
#include "../Wal/wal.h"
#include "Compaction.h"

// deklaracija unapred da se izbegnu ciklicne zavisnosti
class CompactionStrategy;

class LSMManager {
public:
    LSMManager(SSTManager* sstManager, Config* config);
    // konstruktor sada prima odabranu strategiju i maksimalan broj nivoa
    //LSMManager(std::unique_ptr<CompactionStrategy> strategy, int max_levels, SSTManager& sstRef);

    // destruktor se brine o bezbednom zaustavljanju pozadinske niti
    ~LSMManager();

    // Glavna funkcija koja se poziva nakon flush-a iz Memtable-a.
    // Pokreće proveru i, ako je potrebno, lančanu kompakciju kroz sve nivoe.
    void triggerCompactionCheck();

private:
    Config* config;
};