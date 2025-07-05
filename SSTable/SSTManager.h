#pragma once

#include <string>
#include <vector>
#include <filesystem>
#include "../Wal/wal.h"
#include "../LSM/Compaction.h"

class SSTManager
{
private:

    // osnovni direktorijum, npr. "../data"

    std::string directory_;
    // Format imena: filter_x.sst, summary_x.sst, index_x.sst, sstable_x.sst, meta_x.sst   

    // pronalazenje jedinsvenog ID-a za novi SSTable
    int findNextIndex(const std::string& levelDir) const;

    uint32_t next_ID_map;
    unordered_map<string, uint32_t> key_map;
    void writeMap() const;
    void readMap();

    Block_manager& bm;
    int block_size;

    bool readBytes(void *dst, size_t n, uint64_t& offset, string fileName) const;

public:

    SSTManager(const std::string& directory, Block_manager& bm);
    ~SSTManager();

    // NAPOMENA: ovu metodu bi trebalo imati u LSMManager jer on treba da upravlja po nivoima
    // TODO: takodje, nije dobro da vraca "" ako je key obrisan ili ga nema. Napraviti da radi kao memtable optional<string> get(key, bool& deleted);
    std::string get(const std::string& key);

    SSTableMetadata write(std::vector<Record> sortedRecords, int level);
};
