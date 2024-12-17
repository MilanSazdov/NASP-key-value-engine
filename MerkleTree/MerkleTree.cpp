
#include "MerkleTree.h"

MerkleTree::MerkleTree(const std::vector<std::string>& data) {
    // Generisanje listova - hash podataka
	// Generating leaves - hash of the data
    for (const auto& value : data) {
        leaves.push_back(hash(value));
    }

    // Gradimo stablo i dobijamo root hash
	// Building the tree and getting the root hash
    rootHash = buildTree(leaves);
}

std::string MerkleTree::getRootHash() const {
	return rootHash;
}

std::string MerkleTree::hash(const std::string& data) const {
	std::hash<std::string> hasher;
	return std::to_string(hasher(data));
}

std::string MerkleTree::buildTree(std::vector<std::string> hashes) {
    if (hashes.empty()) return "";

    while (hashes.size() > 1) {
        std::vector<std::string> nextLevel;

        for (size_t i = 0; i < hashes.size(); i += 2) {
            std::string left = hashes[i];
            std::string right = (i + 1 < hashes.size()) ? hashes[i + 1] : left;

            
            nextLevel.push_back(hash(left + right));
        }
        hashes = nextLevel;
    }

    return hashes.front();
}