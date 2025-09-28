#include "hll.h"
#include <iostream>
#include <vector>
#include <string>
#include <iomanip>
#include <bitset>

using namespace std;

std::string toBinaryString(uint32_t value) {
    return std::bitset<32>(value).to_string(); 
}

// Funkcija za ispis registra sa razmacima izmecu bajtova
void printRegister(const std::vector<uint8_t>& reg) {
    std::cout << "Registar: ";
    for (const auto& regValue : reg) {
        int value = static_cast<int>(regValue);

        int colorCode;
        if (value == 0) {
            colorCode = 255;  // Bijela za 0
        }
        else {
            colorCode = 242;
        }

        std::cout << "\033[38;5;" << colorCode << "m"
            << value << "\033[0m ";
    }
    std::cout << std::endl;
}

void addTestData(HyperLogLog& hll, const std::vector<std::string>& elements) {
    for (const auto& element : elements) {
        hll.add(element);
    }
}

void addTestApple(HyperLogLog& hll, const std::vector<std::string>& elements) {
    hll.add("apple");
    hll.add("apple");
    hll.add("apple");
}

void addTestDataAndPrint(HyperLogLog& hll, const std::vector<std::string>& elements) {
    cout << "Registar prije dodavanja:" << endl;
    printRegister(hll.getReg());
    cout << endl;

    for (const auto& element : elements) {
        std::cout << "Dodavanje elementa: " << element << std::endl;

        uint32_t hashValue = hll.getHash(element);

        uint8_t precision = hll.getPrecision();
        uint32_t key = hashValue >> (32 - precision);

        uint8_t trailingZeros = 1;
        uint32_t mask = 1;

        while ((hashValue & mask) == 0 && trailingZeros < 255) {
            trailingZeros++;
            mask <<= 1;
        }

        std::cout << "Hash (bin): ";
        std::string binaryStr = std::bitset<32>(hashValue).to_string();

        std::cout << "\033[33m" << binaryStr.substr(0, precision) << "\033[0m";

        int middleEnd = 32 - trailingZeros;
        std::cout << binaryStr.substr(precision, middleEnd - precision);

        std::cout << "\033[31m" << binaryStr.substr(middleEnd) << "\033[0m";

        std::cout << "  Key: "
            << "\033[33m" << std::bitset<4>(key) << "\033[0m"
            << " (" << key << ")";

        std::cout << " trailingZeros: " << "\033[31m"
            << static_cast<int>(trailingZeros) << "\033[0m" << std::endl;

        hll.add(element);

        printRegister(hll.getReg());
        std::cout << std::endl;
    }
}

double estimatePrinter(uint64_t M, uint8_t P, const std::vector<uint8_t>& Reg) {
    double sum = 0.0;
    uint64_t emptyRegs = 0;

    for (uint8_t val : Reg) {
        if (val == 0) emptyRegs++;
        sum += 1.0 / (1ull << val);
    }

    std::cout << "Suma inverznih vrijednosti: " << sum << std::endl;
    std::cout << "Broj praznih registara: " << static_cast<int>(emptyRegs) << std::endl;

    double alpha;
    if (P == 4) alpha = 0.673;
    else if (P == 5) alpha = 0.697;
    else if (P == 6) alpha = 0.709;
    else alpha = 0.7213 / (1 + 1.079 / M);

    double estimation = alpha * M * M / sum;
    std::cout << "Osnovna procjena kardinalnosti: " << estimation << std::endl;

    if (emptyRegs > 0 && estimation <= 5 * M) {
        estimation = M * log(static_cast<double>(M) / emptyRegs);
        std::cout << "Korigovana procjena (mali podaci): " << estimation << std::endl;
    }
    else if (estimation > pow(2, 32) / 30) {
        estimation = -pow(2, 32) * log1p(-estimation / pow(2, 32));
        std::cout << "Korigovana procjena (veliki podaci): " << estimation << std::endl;
    }

    std::cout << "Konacna procjena kardinalnosti: " << estimation << std::endl;
    return estimation;
}


void testPrecisionEstimates(const std::vector<std::string>& elements) {
    std::cout << "Testiranje procjene za preciznosti od 4 do 14:\n\n";

    for (uint8_t precision = 4; precision <= 14; ++precision) {
        std::cout << "Preciznost: " << static_cast<int>(precision) << std::endl;

        HyperLogLog hll(precision);

        for (const auto& element : elements) {
            hll.add(element);
        }

        double estimate = estimatePrinter(static_cast<uint64_t>(1u) << hll.getPrecision(), hll.getPrecision(), hll.getReg());
        std::cout << "Procijenjeni broj jedinstvenih elemenata: " << "\033[33m" << estimate << "\033[0m" << "\n\n";
    }
}

int mainc() {
    std::cout << "Testiranje HyperLogLog funkcionalnosti...\n";

    uint8_t precision = 4; // Prilagoditi preciznost po potrebi
    HyperLogLog hll(precision);

    std::cout << "Dodavanje test podataka...\n";
    std::vector<std::string> testData = {       // 22 jedinstvena elementa 
    "apple", "apple", "banana", "carrot", "dog", "elephant", "flower", "grape",
    "apple", "banana", "carrot", "dog", "elephant", "flower", "grape", "horse",
    "ice", "juice", "kiwi", "lemon", "mango", "apple", "orange", "pear",
    "quince", "rose", "strawberry", "grape", "umbrella", "violin", "water",
    "apple", "banana", "carrot", "dog", "elephant", "flower", "grape",
    "horse", "ice", "juice", "kiwi", "lemon", "mango", "orange", "pear",
    "rose", "strawberry", "violin", "water", "zebra", "apple", "banana"
    }; 
    std::vector<std::string> appleData = {"apple"};
    addTestDataAndPrint(hll, testData);
    //addTestApple(hll, appleData);
    addTestData(hll, testData);
    testPrecisionEstimates(testData);

    std::cout << "Procjena kardinalnosti nakon dodavanja podataka...\n";
    double estimate = hll.estimate();
    std::cout << "Procijenjeni broj jedinstvenih elemenata: " << estimate << std::endl;

    std::cout << "Serijalizacija i deserijalizacija HyperLogLog...\n";
    std::vector<byte> serializedData = hll.serialize();
    HyperLogLog deserializedHLL = HyperLogLog::deserialize(serializedData);

    std::cout << "Procjena kardinalnosti nakon deserijalizacije...\n";
    double deserializedEstimate = deserializedHLL.estimate();
    std::cout << "Procijenjeni broj jedinstvenih elemenata (nakon deserijalizacije): " << deserializedEstimate << std::endl;

    return 0;
}