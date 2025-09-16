#pragma once

#include <string>
#include <vector>
#include <filesystem>
#include <optional>
#include "../Wal/wal.h"
#include "SSTable.h"

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
    vector<string> id_to_key;

    void writeMap() const;
    void readMap();

    Block_manager* bm;
    int block_size;

    bool readBytes(void* dst, size_t n, uint64_t& offset, string fileName) const;

public:
    //SSTManager();
    SSTManager(Block_manager* bmp);
    ~SSTManager();

    Block_manager* get_block_manager();
    std::unordered_map<std::string, uint32_t>& get_key_to_id_map();
    std::vector<std::string>& get_id_to_key_map();
    uint32_t& get_next_id();

    optional<string> get(const std::string& key);
    optional<string> get_from_level(const std::string& key, bool& deleted, int level);
	void write(std::vector<Record> sortedRecords, int level);
    vector<unique_ptr<SSTable>> getTablesFromLevel(int level); // -skenira direktorijum za dati nivo, pronalazi sve SSTABLE
	void removeSSTables(int level, const vector<unique_ptr<SSTable>>& tablesToRemove);
};
