#pragma once
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <algorithm>

// TODO: obrisati, ne valja koristiti using namespace std; u .h fajlovima
using namespace std;

class SimHash {
private:
    map<string, bool> stopWords;
    uint64_t hashWord(const string& word) const;
    uint64_t computeFingerprint(const vector<string>& words) const;
public:
    vector<string> parseText(const string& text) const;
    SimHash();
    int hammingDistance(const string& text1, const string& text2) const;
    uint64_t getFingerprint(const string& text) const;
    // TODO: dodati serialize i deserialize (KOJI KORISTE byte, ne u_int8)
};