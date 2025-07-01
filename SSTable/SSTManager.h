#pragma once

#include <string>
#include <vector>
#include <filesystem>
#include "wal.h"
#include "../LSM/Compaction.h"

class SSTManager
{
private:

    // osnovni direktorijum, npr. "../data"

    std::string directory_;
    // Format imena: filter_x.sst, summary_x.sst, index_x.sst, sstable_x.sst, meta_x.sst   

    // pronalazenje jedinsvenog ID-a za novi SSTable
    int findNextIndex(const std::string& levelDir) const;

public:

    SSTManager();

    // NAPOMENA: ovu metodu bi trebalo imati u LSMManager jer on treba da upravlja po nivoima
    // TODO: takodje, nije dobro da vraca "" ako je key obrisan ili ga nema. Napraviti da radi kao memtable optional<string> get(key, bool& deleted);
    std::string get(const std::string& key) const;

    SSTableMetadata write(std::vector<Record> sortedRecords, int level) const;
};
