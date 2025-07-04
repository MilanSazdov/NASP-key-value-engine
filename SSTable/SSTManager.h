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

    optional<string> get(const std::string& key) const;
    optional<string> get_from_level(const std::string& key, bool& deleted, int level) const;

    SSTableMetadata write(std::vector<Record> sortedRecords, int level) const;
};
