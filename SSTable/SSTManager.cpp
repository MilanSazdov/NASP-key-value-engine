#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include "SSTManager.h"
#include "SSTable.h"
#include "SSTableComp.h"
#include "SSTableRaw.h"
#include "BloomFilter.h"

namespace fs = std::filesystem;

using ull = unsigned long long;

SSTManager::SSTManager(const std::string& directory, Block_manager& bm) :
                       directory_(directory), bm(bm), block_size(Config::block_size)
{
    readMap();
}

SSTManager::~SSTManager()
{
    writeMap();
}

void SSTManager::readMap() {
    string key_map_file = directory_ + "key_map";

    uint64_t fileOffset = 0;

    uint64_t count;
    readBytes(&count, sizeof(count), fileOffset, key_map_file);

    key_map.reserve(count);
    
    int i = 0;
    for(i = 0; i < count; i++) {
        uint64_t key_size;
        readBytes(&key_size, sizeof(key_size), fileOffset, key_map_file);
        string key;
        readBytes(&key, key_size, fileOffset, key_map_file);

        key_map[key] = i;
    }
    next_ID_map = i + 1;
}

void SSTManager::writeMap() const {

    string key_map_file = directory_ + "key_map";

    string payload = "";

    uint64_t count = key_map.size();

    payload.append(reinterpret_cast<const char*>(&count), sizeof(count));

    int i = 0;
    for(auto kv : key_map) {
        payload.append(reinterpret_cast<const char*>(kv.first.size()), sizeof(kv.first.size()));
        payload.append(kv.first);
    }

    int block_id = 0;
    uint64_t total_bytes = payload.size();
    uint64_t offset = 0;

    while (offset + block_size <= total_bytes) {
        string chunk = payload.substr(offset, block_size);
        bm.write_block({block_id++, key_map_file}, chunk);
        offset += block_size;
    }

    if (offset < total_bytes) {
        std::string chunk = payload.substr(offset);
        bm.write_block({ block_id++, key_map_file }, chunk);
    }
}

// pomocna funkcija za pronalazenje sledeceg ID-a
int SSTManager::findNextIndex(const std::string& levelDir) const {
    int max_id = -1;
    if (!fs::exists(levelDir) || !fs::is_directory(levelDir)) {
        return 0; // prvi fajl ce biti sa ID-em 0
    }

    for (const auto& entry : fs::directory_iterator(levelDir)) {
        std::string filename = entry.path().filename().string();
        if (filename.rfind("sstable_", 0) == 0 && entry.path().extension() == ".sst") {
            try {
                // Primer: iz "sstable_5.sst" izvuci "5"
                std::string id_str = filename.substr(8, filename.find('.') - 8);
                int id = std::stoi(id_str);
                if (id > max_id) {
                    max_id = id;
                }
            }
            catch (const std::exception& e) {
                // Ignorisi fajlove sa nevalidnim formatom imena
                continue;
            }
        }
    }
    return max_id + 1;
}

// POGLEDATI NAPOMENU IZ .H FAJLA !!!!!!!!!!!!!!
std::string SSTManager::get(const std::string& key)
{
    std::vector<Record> matches;

    for (const auto& entry : fs::directory_iterator(directory_))
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

                    SSTable* sst;

                    if(Config::compress_sstable) {
                        sst = new SSTableComp((directory_ + "sstable_" + ending),
                        (directory_ + "index_" + ending),
                        (directory_ + "filter_" + ending),
                        (directory_ + "summary_" + ending),
                        (directory_ + "meta_" + ending),
                        &bm, key_map, next_ID_map);
                    } else {
                        sst = new SSTableRaw((directory_ + "sstable_" + ending),
                        (directory_ + "index_" + ending),
                        (directory_ + "filter_" + ending),
                        (directory_ + "summary_" + ending),
                        (directory_ + "meta_" + ending), &bm);
                    }

                    //Sve recorde sa odgovarajucim key-em stavljamo u vektor
                    std::vector<Record> found = sst->get(key);
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

SSTableMetadata SSTManager::write(std::vector<Record> sortedRecords, int level) {
    if (sortedRecords.empty()) {
        throw std::runtime_error("[SSTManager] Cannot write an SSTable with zero records.");
    }

    std::string levelDir = directory_ + "/level_" + std::to_string(level);

    // Osiguraj da direktorijum za nivo postoji
    if (!fs::exists(levelDir)) {
        fs::create_directories(levelDir);
    }

    int fileId = findNextIndex(levelDir);
    std::string numStr = std::to_string(fileId);
    std::string ending = "_" + numStr + ".sst"; // Koristimo _ da odvojimo ime i broj

    // Kreiraj putanje za sve komponente SSTable-a
    std::string data_path = levelDir + "/sstable" + ending;
    std::string index_path = levelDir + "/index" + ending;
    std::string filter_path = levelDir + "/filter" + ending;
    std::string summary_path = levelDir + "/summary" + ending;
    std::string meta_path = levelDir + "/meta" + ending;

    // Instanciraj SSTable objekat koji ce obaviti upisivanje

    SSTable* sst;

    if(Config::compress_sstable) {
        sst = new SSTableComp((directory_ + "sstable_" + ending),
        (directory_ + "index_" + ending),
        (directory_ + "filter_" + ending),
        (directory_ + "summary_" + ending),
        (directory_ + "meta_" + ending),
        &bm, key_map, next_ID_map);
    } else {
        sst = new SSTableRaw((directory_ + "sstable_" + ending),
        (directory_ + "index_" + ending),
        (directory_ + "filter_" + ending),
        (directory_ + "summary_" + ending),
        (directory_ + "meta_" + ending), &bm);
    }

    // Ova metoda vrsi "tezak posao" upisivanja svih 5 fajlova.
    sst->build(sortedRecords);

    // Nakon sto su fajlovi upisani, kreiramo i popunjavamo metapodatke.
    SSTableMetadata meta;
    meta.level = level;
    meta.file_id = fileId;
    meta.min_key = sortedRecords.front().key; // Vektor je vec sortiran
    meta.max_key = sortedRecords.back().key;  // Vektor je vec sortiran

    // Sracunaj ukupnu velicinu fajla sabiranjem velicina svih komponenti
    uint64_t total_size = 0;
    try {
        total_size += fs::file_size(data_path);
        total_size += fs::file_size(index_path);
        total_size += fs::file_size(summary_path);
        total_size += fs::file_size(filter_path);
        total_size += fs::file_size(meta_path);
    }
    catch (const fs::filesystem_error& e) {
        std::cerr << "Error getting file size for SSTable " << fileId << ": " << e.what() << std::endl;
        // U slučaju greske, postavi velicinu na 0
        total_size = 0;
    }
    meta.file_size = total_size;

    // Sacuvaj pune putanje do fajlova u metapodacima
    meta.data_path = data_path;
    meta.index_path = index_path;
    meta.summary_path = summary_path;
    meta.filter_path = filter_path;
    meta.meta_path = meta_path;

    std::cout << "[SSTManager] Successfully wrote SSTable " << fileId << " to level " << level
        << " (Total Size: " << total_size << " bytes)." << std::endl;

    return meta;
}

bool SSTManager::readBytes(void *dst, size_t n, uint64_t& offset, string fileName) const
{
    if(n==0) return;

    bool error = false;

    int block_id = offset / block_size;
    uint64_t block_pos = offset % block_size;  

    vector<byte> block = bm.read_block({block_id++, fileName}, error);
    if(error) return !error;

    char* out = reinterpret_cast<char*>(dst);

    while (n > 0) {
        if (block_pos == block_size) {
            // fetch sledeci
            block = bm.read_block({block_id++, fileName}, error);
            if(error) return !error;

            block_pos = 0;
        }
        size_t take = min(n, block_size - block_pos);
        std::memcpy(out, block.data() + block_pos, take);
        out += take;
        offset += take;
        block_pos += take;
        n -= take;
    }

    return !error;
}