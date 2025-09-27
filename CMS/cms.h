// cms.h
#pragma once

#include <vector>
#include <string>
#include <functional>
#include <cstddef>
#include <cstdint>

class CountMinSketch {
public:
    CountMinSketch(double epsilon, double delta);
    CountMinSketch(unsigned int m, unsigned int k, unsigned int timeConst);

    void add(const std::string& elem);
    int query(const std::string& elem) const;
    std::vector<std::byte> serialize() const;
    static CountMinSketch deserialize(const std::vector<std::byte>& data);

private:
    unsigned int m;
    unsigned int k;
    std::vector<std::vector<int>> sketch;
    unsigned int timeConst;
    std::vector<uint32_t> hashSeeds;

    static unsigned int findM(double epsilon);
    static unsigned int findK(double delta);
    static std::vector<uint32_t> createHashSeeds(unsigned int k, unsigned int seed);
    static uint32_t hashElement(const std::string& elem, uint32_t seed, unsigned int m);
};
