#pragma once

#include <vector>
#include <string>
#include <functional>

class MerkleTree {
private:
    std::vector<std::string> leaves; // Heš vrednosti listova
    std::string rootHash;            // Heš vrednost korena stabla
    std::vector<std::vector<std::string>> treeLevels; // Svi nivoi stabla

    // Da bih mogao ne zavisno od bilo koje instance MerkleTree da pozovem
    static std::string hash(const std::string& data);
    std::string buildTree(const std::vector<std::string>& hashes);

public:
    // Konstruktor
    MerkleTree(const std::vector<std::string>& data);

    // Vraća root hash stabla
    std::string getRootHash() const;

    // Dobijanje listova
    std::vector<std::string> getLeaves() const { return leaves; }

    // Kreira dokaz za određeni podatak
    std::vector<std::pair<std::string, bool>> generateProof(const std::string& data) const;

    // Proverava dokaz
    static bool verifyProof(const std::string& rootHash, const std::string& data,
        const std::vector<std::pair<std::string, bool>>& proof);
};
