#include "Config.h"
#include <fstream>
#include <string>
#include <iostream>

int Config::cache_capacity = 0; // This is required
int Config::block_size = 0;     // This is required
int Config::segment_size = 0;   // This is required

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
    std::cout << "Debuging Config class\n";
    std::cout << "segment_size: " << segment_size << std::endl;
    std::cout << "block_size: " << block_size << std::endl;
    std::cout << "cache_capacity: " << cache_capacity << std::endl << std::endl;
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
        if (line.find("cache_capacity") != std::string::npos) {
            cache_capacity = getValueFromLine(line);
        }
        else if (line.find("block_size") != std::string::npos) {
            block_size = getValueFromLine(line);
        }
        else if (line.find("segment_size") != std::string::npos) {
            segment_size = getValueFromLine(line);
        }
    }
    debug();
}