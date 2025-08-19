#pragma once

#include <string>
#include <vector>
#include <filesystem>
#include "../Wal/wal.h"

struct SSTableMetadata {
    int level;
    int file_id;
    uint64_t file_size; // Ukupna veličina svih fajlova za ovaj SSTable
    std::string min_key;
    std::string max_key;

    bool is_compressed;
    bool is_single_file;

    std::string data_path;
    std::string index_path;
    std::string summary_path;
    std::string filter_path;
    std::string meta_path;

    bool operator==(const SSTableMetadata& other) const {
        return file_id == other.file_id && level == other.level;
    }
};

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

    Block_manager& bm;
    int block_size;

    bool readBytes(void* dst, size_t n, uint64_t& offset, string fileName) const;

public:
    //SSTManager();
    SSTManager(Block_manager& bmRef);
    ~SSTManager();

	Block_manager& get_block_manager() { return bm; }
    std::unordered_map<std::string, uint32_t>& get_key_to_id_map() { return key_map; }
    std::vector<std::string>& get_id_to_key_map() { return id_to_key; }
    uint32_t& get_next_id() { return next_ID_map; }

    optional<string> get(const std::string& key);
    optional<string> get_from_level(const std::string& key, bool& deleted, int level);

    SSTableMetadata write(std::vector<Record> sortedRecords, int level);
    
    // TODO: vector<SSTableMetadata> findTablesForLevel(int level) - skenira direktorijum za dati nivo, pronalazi sve SSTABLE

};
