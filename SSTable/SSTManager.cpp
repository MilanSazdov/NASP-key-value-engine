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

                std::vector<byte> fileData(fileSize);

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

void SSTManager::write(std::vector<Record>& sortedRecords, int level) const
{
    std::string levelDir = directory + "/level_" + std::to_string(level);
    
    // Kreiramo direktorijum ako ne postoji
    if (!fs::exists(levelDir)) {
        fs::create_directory(levelDir);
    }
    std::vector<std::string> requiredFiles = {
        "filter_0.sst",
        "index_0.sst",
        "meta_0.sst",
        "sstable_0.sst",
        "summary_0.sst"
    };

    // Check each file and create if missing
    for (const auto& fileName : requiredFiles) {
        fs::path filePath = fs::path(levelDir) / fileName;

        if (!fs::exists(filePath)) {
            std::cout << "File missing: " << fileName << " -> Creating it." << std::endl;
            std::ofstream outfile(filePath);
            if (outfile) {
                outfile << ""; // Optionally write initial content
                outfile.close();
            }
            else {
                std::cerr << "Error creating file: " << fileName << std::endl;
            }
        }
        else {
            std::cout << "File exists: " << fileName << std::endl;
        }
    }

    // Pronalazimo sledeci slobodan indeks u tom izvoru

    cout << "[debug] : Finding next index " << levelDir << endl;
    int num = findNextIndex(levelDir);
    cout << "[debug] : Next index: " << num << endl;

    std::string numStr = to_string(num);
    std::string ending = numStr + ".sst";

    // Kreiramo putanje
    SSTable sst(
        levelDir + "/sstable_" + ending,
        levelDir + "/index_" + ending,
        levelDir + "/filter_" + ending,
        levelDir + "/summary_" + ending,
        levelDir + "/meta_" + ending
    );

    sst.build(sortedRecords);
}