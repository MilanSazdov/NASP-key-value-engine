#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include "SSTManager.h"
#include "SSTable.h"
#include "SSTableComp.h"
#include "SSTableRaw.h"
#include "../BloomFilter/BloomFilter.h"

namespace fs = std::filesystem;

using ull = unsigned long long;

SSTManager::SSTManager(Block_manager* bmRef) : directory_(Config::data_directory), bm(bmRef), block_size(Config::block_size) {
    cout << Config::data_directory << endl;
    readMap();
}

Block_manager* SSTManager::get_block_manager() {
    return bm;
}

std::unordered_map<std::string, uint32_t>& SSTManager::get_key_to_id_map() {
    return key_map;
}

std::vector<std::string>& SSTManager::get_id_to_key_map() {
    return id_to_key;
}

uint32_t& SSTManager::get_next_id() {
    return next_ID_map;
}

SSTManager::~SSTManager()
{
    writeMap();
}

void SSTManager::readMap() {
    string key_map_file = directory_ + "/key_map";

    uint64_t fileOffset = 0;

    uint64_t count;
    if (!readBytes(&count, sizeof(count), fileOffset, key_map_file)) {
        return;
    };
    std::cout << "count: " << count << std::endl;

    key_map.reserve(count);
    id_to_key.reserve(count);

    int i = 0;
    for (i = 0; i < count; i++) {
        uint64_t key_size;
        if (!readBytes(&key_size, sizeof(key_size), fileOffset, key_map_file)) {
            return;
        };
        string key;
        if (!readBytes(&key, key_size, fileOffset, key_map_file)) {
            return;
        };

        key_map[key] = i;
        id_to_key.emplace_back(key);
    }
    next_ID_map = i + 1;
}

void SSTManager::writeMap() const {

    string key_map_file = directory_ + "/key_map";

    string payload = "";

    uint64_t count = key_map.size();

    payload.append(reinterpret_cast<const char*>(&count), sizeof(count));

    int i = 0;
    for(auto kv : key_map) {
        cout << "kvfirstsize == " << kv.first.size() << endl;

        size_t len = kv.first.size();  // get the size
        payload.append(reinterpret_cast<const char*>(&len), sizeof(len));  // append size in bytes
        payload.append(kv.first);  // append the string data
    }

    int block_id = 0;
    uint64_t total_bytes = payload.size();
    uint64_t offset = 0;

    while (offset + block_size <= total_bytes) {
        string chunk = payload.substr(offset, block_size);
        bm->write_block({ block_id++, key_map_file }, chunk);
        offset += block_size;
    }

    if (offset < total_bytes) {
        std::string chunk = payload.substr(offset);
        bm->write_block({ block_id++, key_map_file }, chunk);
    }
}

// pomocna funkcija za pronalazenje sledeceg ID-a
int SSTManager::findNextIndex(const std::string& levelDir) const {
    int max_id = -1;
    if (!fs::exists(levelDir) || !fs::is_directory(levelDir)) {
        return 0; // prvi fajl ce biti sa ID-em 0
    }

    int digit, base=1, current;

    for (const auto& entry : fs::directory_iterator(levelDir)) {
        std::string filename = entry.path().filename().string();
        current = 0;
        base = 1;

        for(int i = filename.size()-1; i >= 0; i--) {
            if (isdigit(filename[i])) {
                // pronasli smo poslednji broj u nazivu fajla
                digit = std::stoi(filename.substr(i));
				current += base * digit;
                base *= 10;
            }
		}
		max_id = max(max_id, current);
        
    }
    return max_id + 1;
}

optional<string> SSTManager::get_from_level(const std::string& key, bool& deleted, int level)
{
    deleted = false;
    string current_directory = Config::data_directory + "/level_" + to_string(level) + "/";
    cout << "current_directory == " << current_directory << endl;

    if (!fs::is_directory(current_directory)) {
        cout << "[SSTManager] Error get_from_level " << current_directory << " is not valid directory\n\n";
        return nullopt;
    }

    if (!fs::exists(current_directory)) return nullopt;

    std::vector<Record> matches;
	string data_path, index_path, filter_path, summary_path, meta_path;

    for (const auto& entry : fs::directory_iterator(current_directory))
    {
        if (entry.is_regular_file()) {
            std::string filename = entry.path().filename().string();
            //cout << filename << endl;
            // Prolazimo kroz sve filter fajlove
            if (filename.rfind("filter", 0) == 0) {
                // Citamo fajl
                std::size_t fileSize = fs::file_size(current_directory + filename);

                std::vector<byte> fileData(fileSize);
                if (fileSize == 0) {
                    cout << "File : " << filename << ".size() == 0\n";
                    continue;
                }
                // TODO: NE SME DA SE KORISTI STREAM, MORA PREKO BLOCK MANAGERA
                std::ifstream file(current_directory + filename, std::ios::binary);
                if (!file) {
                    throw std::runtime_error("[SSTManager] Ne mogu da otvorim fajl: " + filename);
                }

                file.read(reinterpret_cast<char*>(fileData.data()), fileSize);


                BloomFilter bloom = BloomFilter::deserialize(fileData);

                if (bloom.possiblyContains(key)) {
                    // Gledamo koji je broj
                    std::size_t underscorePos = filename.rfind('_');
                    std::size_t dotPos = filename.find('.', underscorePos);

                    if (underscorePos == std::string::npos || dotPos == std::string::npos) {
                        cerr << ("[SSTManager] Ne validan format: " + filename);
                        continue;
                    }

                    std::string numStr = filename.substr(underscorePos + 1, dotPos - underscorePos - 1);
                    std::string ending = numStr + ".db";
					
                    SSTable* sst;

					// OVO UOPSTE NE RADI ZA SINGLE FILE MODE
                    if (Config::compress_sstable) {
						data_path = current_directory + "data_comp_" + ending;
						index_path = current_directory + "index_comp_" + ending;
						filter_path = current_directory + "filter_comp_" + ending;
						summary_path = current_directory + "summary_comp_" + ending;
						meta_path = current_directory + "meta_comp_" + ending;
                        
                        sst = new SSTableComp(data_path,
                                              index_path,
                                              filter_path,
                                              summary_path,
							                  meta_path,
                                              bm, key_map, id_to_key, next_ID_map);
                    }
                    else {
                        data_path = current_directory + "data_raw_" + ending;
                        index_path = current_directory + "index_raw_" + ending;
                        filter_path = current_directory + "filter_raw_" + ending;
                        summary_path = current_directory + "summary_raw_" + ending;
                        meta_path = current_directory + "meta_raw_" + ending;

                        sst = new SSTableRaw(data_path,
                                             index_path,
                                             filter_path,
                                             summary_path,
                                             meta_path,
							                 bm);
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


    // not found. Return nullopt
    if (tsMax == 0) return nullopt;

    // found. Record is deleted. Return nullopt + DELETED = TRUE
    else if (static_cast<int>(rMax.tombstone) == 1)
    {
        deleted = true;
        return nullopt;
    }

    // found. Return value 
    return rMax.value;
}

// pretrazuje sstable po nivoima. Kad naide na nekom nivou na kljuc, to vraca. Kad prodje sve nivoe, ne postoji kljuc, vraca nullopt
optional<string> SSTManager::get(const std::string& key) {
    bool deleted;
    for (int i = 1; i <= Config::max_levels; i++) {
        auto ret = get_from_level(key, deleted, i);
        // record doesnt exists BECAUSE IT IS DELETED
        if (deleted) return nullopt;
        // record exists
        else if (ret != nullopt) return ret.value();
    }
    //record doesnt exists because IT WASNT PUT
    return nullopt;
}

void SSTManager::write(std::vector<Record> sortedRecords, int level) {
    if (sortedRecords.empty()) {
        throw std::runtime_error("[SSTManager] Cannot write an SSTable with zero records.");
    }

    std::string levelDir = directory_ + "/level_" + std::to_string(level);

    // Osiguraj da direktorijum za nivo postoji
    if (!fs::exists(levelDir)) {
        fs::create_directories(levelDir);
    }

    int fileId = findNextIndex(levelDir);
	cout << "[SSTManager] Writing SSTable to level " << level << " with file ID: " << fileId << endl;

    std::string numStr = std::to_string(fileId);

    // Ovde treba da se handluje ako je jedan file
    bool use_single_file = Config::sstable_single_file;
    bool use_compression = Config::compress_sstable;

    SSTable* sst = nullptr;
    std::string data_path, index_path, filter_path, summary_path, meta_path;

    if (use_single_file) {
        std::string type_str = use_compression ? "_sf_comp_" : "_sf_raw_";
        data_path = levelDir + "/sstable" + type_str + numStr + ".db";
        index_path = filter_path = summary_path = meta_path = data_path;
        std::cout << "[SSTManager] Creating a SINGLE-FILE SSTable: " << data_path << std::endl;
    }
    else {
        std::string type_str = use_compression ? "_comp_" : "_raw_";
        std::string extension = ".db";

        data_path = levelDir + "/data" + type_str + numStr + extension;
        index_path = levelDir + "/index" + type_str + numStr + extension;
        filter_path = levelDir + "/filter" + type_str + numStr + extension;
        summary_path = levelDir + "/summary" + type_str + numStr + extension;
        meta_path = levelDir + "/meta" + type_str + numStr + extension;

        std::cout << "[SSTManager] Creating a MULTI-FILE SSTable (ID: " << numStr << ")" << std::endl;

    }

    // TODO: PROVERITI DA LI TREBA I SINGLE FILE DA SE SALJE U CONSTRUCTOR I KOD COMP I KOD RAW
    if (use_compression) {
        sst = new SSTableComp(
            data_path,
            index_path,
            filter_path,
            summary_path,
            meta_path,
            bm,
            key_map,
            id_to_key,
            next_ID_map
        );
    }
    else {
        sst = new SSTableRaw(
            data_path,
            index_path,
            filter_path,
            summary_path,
            meta_path,
            bm
        );
    }

    sst->build(sortedRecords);

    /*
    // Nakon sto su fajlovi upisani, kreiramo i popunjavamo metapodatke.
    SSTableMetadata meta;
    meta.level = level;
    meta.file_id = fileId;
    meta.min_key = sortedRecords.front().key; // Vektor je vec sortiran
    meta.max_key = sortedRecords.back().key;  // Vektor je vec sortiran

    // Sracunaj ukupnu velicinu fajla sabiranjem velicina svih komponenti
    uint64_t total_size = 0;
    try {
        if (use_single_file) {
            // U single-file modu, samo uzimamo veličinu tog jednog fajla
            total_size = fs::file_size(data_path);
        }
        else {
            // U multi-file modu, sabiramo veličine svih 5 fajlova
            total_size += fs::file_size(data_path);
            total_size += fs::file_size(index_path);
            total_size += fs::file_size(summary_path);
            total_size += fs::file_size(filter_path);
            total_size += fs::file_size(meta_path);
        }
    }
    catch (const fs::filesystem_error& e) {
        std::cerr << "Error getting file size for SSTable " << fileId << ": " << e.what() << std::endl;
        total_size = 0;
    }
    meta.file_size = total_size;

    // Sacuvaj pune putanje do fajlova u metapodacima
    meta.data_path = data_path;
    meta.index_path = index_path;
    meta.summary_path = summary_path;
    meta.filter_path = filter_path;
    meta.meta_path = meta_path;


    delete sst; // Oslobodi memoriju
    */
    std::cout << "[SSTManager] Successfully wrote SSTable " << fileId << " to level " << level
        << " (Total Size: [NISAM IZRACUNO]" << " bytes)." << std::endl;
    return;
}

bool SSTManager::readBytes(void* dst, size_t n, uint64_t& offset, string fileName) const
{
    if (n == 0) return true;

    bool error = false;

    int block_id = offset / block_size;
    uint64_t block_pos = offset % block_size;

    vector<byte> block = bm->read_block({ block_id++, fileName }, error);
    if (error) return !error;

    char* out = reinterpret_cast<char*>(dst);

    while (n > 0) {
        if (block_pos == block_size) {
            // fetch sledeci
            block = bm->read_block({ block_id++, fileName }, error);
            if (error) return !error;

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

vector<unique_ptr<SSTable>> SSTManager::getTablesFromLevel(int level) {
	string path = directory_ + "/level_" + to_string(level) + "/";

	// ne postoji level direktorijum ili nije direktorijum
    if (!fs::exists(path) || !fs::is_directory(path)) {
        std::cerr << "[SSTManager] Level directory does not exist: " << path << std::endl;
        return {};
	}

    vector<unique_ptr<SSTable>> tables;
                       
    // nesto + _raw_ + id + .db
    // nesto + _sf_raw_ + id + .db
    for (const auto& entry : fs::directory_iterator(path)) {
        if (entry.is_regular_file()) {
            string filename = entry.path().filename().string();

            // ovo je single file
            if (filename.rfind("sstable_", 0) == 0) {
                // Proveri da li je raw ili comp
                //sstable_sf_raw_0
                if (filename.find("_raw_") != std::string::npos) {
                    tables.push_back(make_unique<SSTableRaw>(filename, bm));
                }
                //sstable_sf_comp_0
                else {
                    tables.push_back(make_unique<SSTableComp>(filename, bm, key_map, id_to_key, next_ID_map));
                }
            }
            // ovde je multi file, treba naci index, meta, summary, filter, data
                
            if (filename.rfind("data", 0) == 0) {
                // Proveri da li je raw ili comp
                if (filename.find("_raw_") != std::string::npos) {
                    //data_raw_32.db
					std::string numStr = filename.substr(9, filename.find(".db") -9);
					int num = std::stoi(numStr);
                    std::string data_path =     "data_raw_" + numStr + ".db";
                    std::string index_path =    "index_raw_" + numStr + ".db";
                    std::string filter_path =   "filter_raw_" + numStr + ".db";
                    std::string summary_path =  "summary_raw_" + numStr + ".db";
                    std::string meta_path =     "meta_raw_" + numStr + ".db";
                    
                    tables.push_back(make_unique<SSTableRaw>(data_path, index_path, filter_path, summary_path, meta_path, bm));
                }
                else {
                    //data_comp_32.db
                    std::string numStr = filename.substr(10, filename.find(".db") - 10);
                    int num = std::stoi(numStr);
                    std::string data_path = "data_comp_" + numStr + ".db";
                    std::string index_path = "index_comp_" + numStr + ".db";
                    std::string filter_path = "filter_comp_" + numStr + ".db";
                    std::string summary_path = "summary_comp_" + numStr + ".db";
                    std::string meta_path = "meta_comp_" + numStr + ".db";
                   
                    tables.push_back(make_unique<SSTableComp>
                        (data_path, index_path, filter_path, summary_path, meta_path, bm, key_map, id_to_key, next_ID_map)
                    );
                }
            }
        }
	}

    return tables;
}