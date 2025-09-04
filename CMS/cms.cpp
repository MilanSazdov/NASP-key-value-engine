#include "cms.h"
#include "../MurmurHash3/MurmurHash3.h"
#include <ctime>
#include <cmath>
#include <algorithm>
#include <cstring>
#include <climits>

using namespace std;

CountMinSketch::CountMinSketch(double epsilon, double delta) {
    m = findM(epsilon);
    k = findK(delta);
    sketch.resize(k, vector<int>(m, 0));
    timeConst = static_cast<unsigned int>(time(nullptr));
    hashSeeds = createHashSeeds(k, timeConst);
}

CountMinSketch::CountMinSketch(unsigned int m, unsigned int k, unsigned int timeConst)
    : m(m), k(k), timeConst(timeConst) {
    sketch.resize(k, vector<int>(m, 0));
    hashSeeds = createHashSeeds(k, timeConst);
}

void CountMinSketch::add(const string& elem) {
    for (unsigned int i = 0; i < k; ++i) {
        uint32_t j = hashElement(elem, hashSeeds[i], m);
        sketch[i][j] += 1;
    }
}

int CountMinSketch::query(const string& elem) const {
    int minCount = INT_MAX;
    for (unsigned int i = 0; i < k; ++i) {
        uint32_t j = hashElement(elem, hashSeeds[i], m);
        minCount = min(minCount, sketch[i][j]);
    }
    return minCount;
}

vector<uint32_t> CountMinSketch::createHashSeeds(unsigned int k, unsigned int seed) {
    vector<uint32_t> seeds;
    seeds.reserve(k);
    for (unsigned int i = 0; i < k; ++i) {
        seeds.push_back(seed + i);
    }
    return seeds;
}

uint32_t CountMinSketch::hashElement(const string& elem, uint32_t seed, unsigned int m) {
    uint32_t hash;
    MurmurHash3_x86_32(elem.c_str(), elem.length(), seed, &hash);
    return hash % m;
}


vector<byte> CountMinSketch::serialize() const {
    vector<byte> data;

    // Serialize m and k
    data.insert(data.end(), reinterpret_cast<const byte*>(&m), reinterpret_cast<const byte*>(&m) + sizeof(m));
    data.insert(data.end(), reinterpret_cast<const byte*>(&k), reinterpret_cast<const byte*>(&k) + sizeof(k));
    
	// Serialize time constant
    data.insert(data.end(), reinterpret_cast<const byte*>(&timeConst), reinterpret_cast<const byte*>(&timeConst) + sizeof(timeConst));

    // Serialize set
    for (const auto& row : sketch) {
        for (int val : row) {
            data.insert(data.end(), reinterpret_cast<const byte*>(&val), reinterpret_cast<const byte*>(&val) + sizeof(val));
        }
    }

    return data;
}

CountMinSketch CountMinSketch::deserialize(const vector<byte>& data) {
    size_t offset = 0;

    unsigned int m, k;
    unsigned int timeConst;

    memcpy(&m, data.data() + offset, sizeof(m));
    offset += sizeof(m);

    memcpy(&k, data.data() + offset, sizeof(k));
    offset += sizeof(k);

    memcpy(&timeConst, data.data() + offset, sizeof(timeConst));
    offset += sizeof(timeConst);

    CountMinSketch cms(m, k, timeConst);

    // Deserialize set
    for (unsigned int i = 0; i < k; ++i) {
        for (unsigned int j = 0; j < m; ++j) {
            memcpy(&cms.sketch[i][j], data.data() + offset, sizeof(cms.sketch[i][j]));
            offset += sizeof(cms.sketch[i][j]);
        }
    }

    return cms;
}

unsigned int CountMinSketch::findM(double epsilon) {
    return static_cast<unsigned int>(ceil(exp(1) / epsilon));
}

unsigned int CountMinSketch::findK(double delta) {
    return static_cast<unsigned int>(ceil(log(exp(1) / delta)));
}
