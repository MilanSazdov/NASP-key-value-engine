
#include "Config.h"
#include <fstream>
#include <string>
#include <iostream>
#include <stdexcept>
#include <algorithm>
#include <iomanip>
#include <cstdint>

// ovaj deo mora da postoji zato sto su static polja. Inace, vrednosti se mogu ignorisati jer postoji defaultConfig, iz njega se uzimaju vrednosti
int Config::cache_capacity = 10;        // 10 elements
int Config::block_size = 50;           // 100 bytes
int Config::segment_size = 5;           // 5 blocks

std::string Config::data_directory = "../data";
std::string Config::wal_directory = "../data/wal_logs";

std::string Config::memtable_type = "hash_map";
size_t Config::memtable_instances = 3;
size_t Config::memtable_max_size = 5;

std::string Config::compaction_strategy = "leveled";
int Config::max_levels = 7;

int Config::l0_compaction_trigger = 4;
int Config::level_size_multiplier = 10;

bool Config::compress_sstable = true;
int Config::index_sparsity = 32;
int Config::summary_sparsity = 64;
bool Config::sstable_single_file = false; // Default je multi-file

int Config::max_tokens = 20;
int Config::refill_interval = 10;

int getValueFromLine(const std::string& line) {
    size_t colonPos = line.find(':');
    if (colonPos == std::string::npos) return -1;

    std::string valuePart = line.substr(colonPos + 1);
    // Remove possible comma and spaces
    valuePart.erase(remove(valuePart.begin(), valuePart.end(), ','), valuePart.end());
    valuePart.erase(remove(valuePart.begin(), valuePart.end(), ' '), valuePart.end());

    std::cout << valuePart << std::endl;

    return std::stoi(valuePart);
}

void Config::debug() {
    std::cout << "Debuging\n";
    const std::string cyan = "\033[36m";
    const std::string bold = "\033[1m";
    const std::string reset = "\033[0m";

    std::cout << bold << cyan << "[Config]" << reset << " Configuration Debug Info\n";
    std::cout << std::left << std::setw(30) << "  segment_size:" << segment_size << "\n";
    std::cout << std::left << std::setw(30) << "  block_size:" << block_size << "\n";
    std::cout << std::left << std::setw(30) << "  cache_capacity:" << cache_capacity << "\n\n";

    std::cout << std::left << std::setw(30) << "  memtable_type:" << memtable_type << "\n";
    std::cout << std::left << std::setw(30) << "  memtable_instances:" << memtable_instances << "\n";
    std::cout << std::left << std::setw(30) << "  memtable_max_size:" << memtable_max_size << "\n\n";

    std::cout << std::left << std::setw(30) << "  compaction_strategy:" << compaction_strategy << "\n";
    std::cout << std::left << std::setw(30) << "  max_levels:" << max_levels << "\n";
    std::cout << std::left << std::setw(30) << "  l0_compaction_trigger:" << l0_compaction_trigger << "\n";
    std::cout << std::left << std::setw(30) << "  level_size_multiplier:" << level_size_multiplier << "\n";

    std::cout << std::left << std::setw(30) << "  wal_directory:" << wal_directory << "\n";
    std::cout << std::left << std::setw(30) << "  data_directory:" << data_directory << "\n\n";

    std::cout << std::left << std::setw(30) << "  max_tokens:" << max_tokens << "\n";
    std::cout << std::left << std::setw(30) << "  refill_interval:" << refill_interval << "\n";
}

void remove_white_space_or_coma(std::string& s) {
    if (s[s.size() - 1] == ',')
        s.pop_back();

    while (s[0] == ' ')
        s = s.substr(1);
}

void Config::load_old_configuration() {
    // Open the file
    std::ifstream file("../defaultConfig.json");
    if (!file.is_open()) {
        std::cerr << "Failed to open default config file." << std::endl;
        return;
    }

    std::string line;

    while (std::getline(file, line)) {
        //std::cout << line << std::endl;
        if (line.find("cache_capacity") != std::string::npos) {
            cache_capacity = getValueFromLine(line);
        }
        else if (line.find("block_size") != std::string::npos) {
            block_size = getValueFromLine(line);
        }
        else if (line.find("segment_size") != std::string::npos) {
            segment_size = getValueFromLine(line);
        }

        else if (line.find("memtable_type") != std::string::npos) {
            memtable_type = line.substr(line.find(':') + 1);
            memtable_type.erase(remove(memtable_type.begin(), memtable_type.end(), '\"'), memtable_type.end());
            remove_white_space_or_coma(memtable_type);
        }
        else if (line.find("memtable_instances") != std::string::npos) {
            memtable_instances = getValueFromLine(line);
        }
        else if (line.find("memtable_max_size") != std::string::npos) {
            memtable_max_size = getValueFromLine(line);
        }
        else if (line.find("compaction_strategy") != std::string::npos) {
            compaction_strategy = line.substr(line.find(':') + 1);
            compaction_strategy.erase(remove(compaction_strategy.begin(), compaction_strategy.end(), '\"'), compaction_strategy.end());
            remove_white_space_or_coma(compaction_strategy);
        }
        else if (line.find("max_levels") != std::string::npos) {
            max_levels = getValueFromLine(line);
        }
        else if (line.find("l0_compaction_trigger") != std::string::npos) {
            l0_compaction_trigger = getValueFromLine(line);
        }

        else if (line.find("level_size_multiplier") != std::string::npos) {
            level_size_multiplier = getValueFromLine(line);
        }

        else if (line.find("data_directory") != std::string::npos) {
            //std::cout << line << std::endl;
            data_directory = line.substr(line.find(':') + 1);
            data_directory.erase(remove(data_directory.begin(), data_directory.end(), '\"'), data_directory.end());
            remove_white_space_or_coma(data_directory);
        }
        else if (line.find("wal_directory") != std::string::npos) {
            wal_directory = line.substr(line.find(':') + 1);
            wal_directory.erase(remove(wal_directory.begin(), wal_directory.end(), '\"'), wal_directory.end());
            remove_white_space_or_coma(wal_directory);
        }
        else if (line.find("compress_sstable") != std::string::npos) {
            compress_sstable = (bool)getValueFromLine(line);
        }
        else if (line.find("sstable_single_file") != std::string::npos) {
            sstable_single_file = (bool)getValueFromLine(line);
        }
        else if (line.find("summary_sparsity") != std::string::npos) {
            summary_sparsity = getValueFromLine(line);
        }
        else if (line.find("index_sparsity") != std::string::npos) {
            index_sparsity = getValueFromLine(line);
        }
    }
}

void writeDefaultConfig() {
    std::cout << "[Config] Writing default configuration to config.json\n";
    std::ofstream out("../defaultConfig.json");
    if (!out.is_open()) {
        std::cerr << "Failed to open defaultConfig.json for writing." << std::endl;
        return;
    }
    out << "{\n";
    out << "  \"cache_capacity\": " << Config::cache_capacity << ",\n";
    out << "  \"block_size\": " << Config::block_size << ",\n";
    out << "  \"segment_size\": " << Config::segment_size << ",\n";
    out << "  \"data_directory\": \"" << Config::data_directory << "\",\n";
    out << "  \"wal_directory\": \"" << Config::wal_directory << "\",\n";
    out << "  \"memtable_type\": \"" << Config::memtable_type << "\",\n";
    out << "  \"memtable_instances\": " << Config::memtable_instances << ",\n";
    out << "  \"memtable_max_size\": " << Config::memtable_max_size << ",\n";
    out << "  \"compaction_strategy\": \"" << Config::compaction_strategy << "\",\n";
    out << "  \"max_levels\": " << Config::max_levels << ",\n";
    out << "  \"l0_compaction_trigger\": " << Config::l0_compaction_trigger << ",\n";
    out << "  \"level_size_multiplier\": " << Config::level_size_multiplier << ",\n";
    out << "  \"index_sparsity\": " << Config::index_sparsity << ",\n";
    out << "  \"summary_sparsity\": " << Config::summary_sparsity << ",\n";
    out << "  \"compress_sstable\": " << (Config::compress_sstable ? 1 : 0) << ",\n";
    out << "  \"sstable_single_file\": " << (Config::sstable_single_file ? 1 : 0) << ",\n";
    out << "  \"max_tokens\": " << Config::max_tokens << ",\n";
    out << "  \"refill_interval\": " << Config::refill_interval << "\n";
    out << "}\n";
    out.close();
}

bool Config::load_init_configuration() {
	load_old_configuration();

    // Open the file
    std::ifstream file("../config.json");
    if (!file.is_open()) {
        std::cerr << "Failed to open config file." << std::endl;
        return false;
    }

    std::string line;

    std::string new_val;
    int new_int;
	bool new_configuration = 0;

    while (std::getline(file, line)) {
        //std::cout << line << std::endl;
        if (line.find("cache_capacity") != std::string::npos) {
            new_int = getValueFromLine(line);
            if (cache_capacity != new_int) {
                new_configuration = 1;
            }
            cache_capacity = new_int;
        }
        else if (line.find("block_size") != std::string::npos) {
            new_int = getValueFromLine(line);
            if (block_size != new_int) {
                new_configuration = 1;
            }
            block_size = new_int;
        }
        else if (line.find("segment_size") != std::string::npos) {
            new_int = getValueFromLine(line);
            if (segment_size != new_int) {
                new_configuration = 1;
            }
            segment_size = new_int;
        }

        else if (line.find("memtable_type") != std::string::npos) {
            new_val = line.substr(line.find(':') + 1);
            new_val.erase(remove(new_val.begin(), new_val.end(), '\"'), new_val.end());
            remove_white_space_or_coma(new_val);
            if (memtable_type != new_val) {
                new_configuration = 1;
            }
            memtable_type = new_val;
        }
        else if (line.find("memtable_instances") != std::string::npos) {
            new_int = getValueFromLine(line);
            if (memtable_instances != new_int) {
                new_configuration = 1;
            }
            memtable_instances = new_int;
        }
        else if (line.find("memtable_max_size") != std::string::npos) {
            new_int = getValueFromLine(line);
            if (memtable_max_size != new_int) {
                new_configuration = 1;
            }
            memtable_max_size = new_int;
        }
        else if (line.find("compaction_strategy") != std::string::npos) {
            new_val = line.substr(line.find(':') + 1);
            new_val.erase(remove(new_val.begin(), new_val.end(), '\"'), new_val.end());
            remove_white_space_or_coma(new_val);
            if (compaction_strategy != new_val) {
                new_configuration = 1;
            }
            compaction_strategy = new_val;
        }
        else if (line.find("max_levels") != std::string::npos) {
            new_int = getValueFromLine(line);
            if (max_levels != new_int) {
                new_configuration = 1;
            }
            max_levels = new_int;
        }
        else if (line.find("l0_compaction_trigger") != std::string::npos) {
            new_int = getValueFromLine(line);
            if (l0_compaction_trigger != new_int) {
                new_configuration = 1;
            }
            l0_compaction_trigger = new_int;
        }
        
        else if (line.find("level_size_multiplier") != std::string::npos) {
            new_int = getValueFromLine(line);
            if (level_size_multiplier != new_int) {
                new_configuration = 1;
            }
            level_size_multiplier = new_int;
        }
        
        else if (line.find("data_directory") != std::string::npos) {
            //std::cout << line << std::endl;
            new_val = line.substr(line.find(':') + 1);
            new_val.erase(remove(new_val.begin(), new_val.end(), '\"'), new_val.end());
            remove_white_space_or_coma(new_val);
            if (data_directory != new_val) {
                new_configuration = 1;
            }
            data_directory = new_val;
        }
        else if (line.find("wal_directory") != std::string::npos) {
            new_val = line.substr(line.find(':') + 1);
            new_val.erase(remove(new_val.begin(), new_val.end(), '\"'), new_val.end());
            remove_white_space_or_coma(new_val);
            if (wal_directory != new_val) {
                new_configuration = 1;
            }
            wal_directory = new_val;
        }
        else if (line.find("compress_sstable") != std::string::npos) {
            new_int = getValueFromLine(line);
            if (compress_sstable != new_int) {
                new_configuration = 1;
            }
            compress_sstable = new_int;
        }
        else if (line.find("sstable_single_file") != std::string::npos) {
            new_int = getValueFromLine(line);
            if (sstable_single_file != new_int) {
                new_configuration = 1;
            }
            sstable_single_file = new_int;
        }
        else if (line.find("summary_sparsity") != std::string::npos) {
            new_int = getValueFromLine(line);
            if (summary_sparsity != new_int) {
                new_configuration = 1;
            }
            summary_sparsity = new_int;
        }
        else if (line.find("index_sparsity") != std::string::npos) {
            new_int = getValueFromLine(line);
            if (index_sparsity != new_int) {
                new_configuration = 1;
            }
            index_sparsity = new_int;
        }
    }

    debug();
    
    if (new_configuration) {
        writeDefaultConfig();
    }

    return new_configuration;
}