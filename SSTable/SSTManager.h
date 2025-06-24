#pragma once

#include <string>
#include <vector>
#include <filesystem>
#include "wal.h"

class SSTManager
{
private:
    std::string directory;
    // Format imena: filter_x.sst, summary_x.sst, index_x.sst, sstable_x.sst, meta_x.sst   

    int findNextIndex(const std::string& levelDir) const
    {
        int freeIndex = 0;
        namespace fs = std::filesystem;
        while (true) {
            fs::path filePath = fs::path(levelDir) / ("sstable_" + std::to_string(freeIndex) + ".sst");
            if (!fs::exists(filePath)) {
                return freeIndex;
            }
            ++freeIndex;
        }
    }

public:

    SSTManager(const std::string& directory)
        : directory(directory) {}

    std::string get(const std::string& key) const;
    void write(std::vector<Record>& sortedRecords, int level) const;
};
