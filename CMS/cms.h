// cms.h
#pragma once
#include <vector>
#include <string>
#include <functional>

using namespace std;

class CountMinSketch {
public:
    CountMinSketch(double epsilon, double delta);
    CountMinSketch(unsigned int m, unsigned int k, unsigned int timeConst);

    void add(const string& elem);
    int query(const string& elem) const;
    vector<uint8_t> serialize() const;
    static CountMinSketch deserialize(const vector<uint8_t>& data);

private:
    unsigned int m;
    unsigned int k;
    vector<vector<int>> sketch;
    unsigned int timeConst;
    vector<uint32_t> hashSeeds; 

    static unsigned int findM(double epsilon);
    static unsigned int findK(double delta);
    static vector<uint32_t> createHashSeeds(unsigned int k, unsigned int seed);
    static uint32_t hashElement(const string& elem, uint32_t seed, unsigned int m);
};