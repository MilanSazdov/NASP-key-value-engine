#include "Config.h"
#include <fstream>
#include <string>
#include <iostream>
#include <stdexcept>
#include <algorithm>
#include <iomanip>

int Config::cache_capacity = 10;        // 10 elements
int Config::block_size = 100;           // 100 bytes
int Config::segment_size = 5;           // 5 blocks

std::string Config::data_directory = "../data";
std::string Config::wal_directory = "../data/wal_logs";

std::string Config::memtable_type = "hash_map";
size_t Config::memtable_instances = 3;
size_t Config::memtable_max_size = 5;

std::string Config::compaction_strategy = "leveled";
int Config::max_levels = 7;

int Config::l0_compaction_trigger = 4;
uint64_t Config::target_file_size_base = 2097152; // 2MB
int Config::level_size_multiplier = 10;

int Config::min_threshold = 4;
int Config::max_threshold = 32;

int getValueFromLine(const std::string& line) {
    size_t colonPos = line.find(':');
    if (colonPos == std::string::npos) return -1;

    std::string valuePart = line.substr(colonPos + 1);
    // Remove possible comma and spaces
    valuePart.erase(remove(valuePart.begin(), valuePart.end(), ','), valuePart.end());
    valuePart.erase(remove(valuePart.begin(), valuePart.end(), ' '), valuePart.end());

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
    std::cout << std::left << std::setw(30) << "  target_file_size_base:" << target_file_size_base << "\n";
    std::cout << std::left << std::setw(30) << "  level_size_multiplier:" << level_size_multiplier << "\n";
    std::cout << std::left << std::setw(30) << "  min_threshold:" << min_threshold << "\n";
    std::cout << std::left << std::setw(30) << "  max_threshold:" << max_threshold << "\n";

    std::cout << std::left << std::setw(30) << "  wal_directory:" << wal_directory << "\n";
    std::cout << std::left << std::setw(30) << "  data_directory:" << data_directory << "\n";
}

void remove_white_space_or_coma(std::string& s) {
    if (s[s.size() - 1] == ',')
        s.pop_back();

    while (s[0] == ' ')
        s = s.substr(1);
}

void Config::load_init_configuration() {
    // Open the file
    std::ifstream file("../Config/config.json");
    if (!file.is_open()) {
        std::cerr << "Failed to open config file." << std::endl;
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
		else if (line.find("target_file_size_base") != std::string::npos) {
			target_file_size_base = static_cast<uint64_t>(getValueFromLine(line));
		}
		else if (line.find("level_size_multiplier") != std::string::npos) {
			level_size_multiplier = getValueFromLine(line);
		}
		else if (line.find("min_threshold") != std::string::npos) {
			min_threshold = getValueFromLine(line);
		}
		else if (line.find("max_threshold") != std::string::npos) {
			max_threshold = getValueFromLine(line);
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
    }
    debug();
}