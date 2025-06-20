#pragma once
#include <string>
#include <vector>
#include <cmath>
#include <cstdint>

// TODO: using namespace nije dobro stavljati u .h fajlove, obrisati 
using namespace std;


class HyperLogLog {
private:
    
    uint64_t M;
    uint8_t P;
    vector<uint8_t> Reg;

public:
    HyperLogLog(uint8_t p);
    void add(const string& word);
    double estimate() const;
    vector<uint8_t> serialize() const;

    // TODO: treba promeniti da se umesto uint8_t koristi byte (u celom hll projektu). Barem gde se to odnosi na data, konkretno
    static HyperLogLog deserialize(const vector<uint8_t>& data);

    uint32_t getHash(const string& text) const;
    vector<uint8_t> getReg() const {
        return Reg;
    }
    uint8_t getPrecision() const {
        return P;
    }
};

