#include <filesystem>
#include <fstream>
#include <iostream>
#include "SSTManager.h"
#include "SSTable.h"
#include "BloomFilter.h"

namespace fs = std::filesystem;

std::string SSTManager::get(const std::string& key) const
{
    std::vector<Record> matches;

    for (const auto& entry : fs::directory_iterator(directory))
    {
        if (entry.is_regular_file()) {
            std::string filename = entry.path().filename().string();

            // Prolazimo kroz sve filter fajlove
            if (filename.rfind("filter", 0) == 0) {

                // Citamo fajl
                std::size_t fileSize = std::filesystem::file_size(filename);

                std::vector<uint8_t> fileData(fileSize);

                std::ifstream file(filename, std::ios::binary);
                if (!file) {
                    throw std::runtime_error("[SSTManager] Ne mogu da otvorim fajl: " + filename);
                }

                file.read(reinterpret_cast<char*>(fileData.data()), fileSize);


                BloomFilter bloom = BloomFilter::deserialize(fileData);

                if (bloom.possiblyContains(key)) {
                    // Gledamo koji je broj
                    std::size_t underscorePos = filename.find('_');
                    std::size_t dotPos = filename.find('.', underscorePos);
                    if (underscorePos == std::string::npos || dotPos == std::string::npos) {
                        cerr << ("[SSTManager] Ne validan format: " + filename);
                        continue;
                    }

                    std::string numStr = filename.substr(underscorePos + 1, dotPos - underscorePos - 1);
                    std::string ending = numStr + ".sst";

                    SSTable sst((directory + "sstable_" + ending),
                        (directory + "index_" + ending),
                        (directory + "filter_" + ending),
                        (directory + "summary_" + ending),
                        (directory + "meta_" + ending));

                    //Sve recorde sa odgovarajucim key-em stavljamo u vektor
                    std::vector<Record> found = sst.get(key);
                    matches.insert(matches.end(), found.begin(), found.end());
                }
            }
        }
    }

    Record rMax;
    ull tsMax = 0;

    for (Record r : matches)
    {
        if (r.timestamp > tsMax)
        {
            tsMax = r.timestamp;
            rMax = r;
        }
    }

    if (static_cast<int>(rMax.tombstone) == 1 || tsMax == 0)
    {
        return "";
    }

    return rMax.value;
}

void SSTManager::write(const std::vector<Record>& sortedRecords) const
{
    int num = findNextIndex();
    std::string numStr = to_string(num);
    std::string ending = numStr + ".sst";

    SSTable sst(("sstable_" + ending),
        ("index_" + ending),
        ("filter_" + ending),
        ("summary_" + ending),
        ("meta_" + ending));

    sst.build(sortedRecords);
}