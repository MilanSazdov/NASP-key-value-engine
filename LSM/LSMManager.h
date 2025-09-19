#pragma once

#include <queue>
#include <vector>
#include <string>
#include <cstdint>
#include <algorithm>
#include <cassert>
#include "../SSTable/SSTManager.h"
#include "../SSTable/SSTable.h"
#include "../SSTable/SSTableComp.h"
#include "../SSTable/SSTableRaw.h"
#include "../Config/Config.h"
#include "..//Wal/wal.h"



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


};