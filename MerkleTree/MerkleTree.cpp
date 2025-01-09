
#include "MerkleTree.h"

MerkleTree::MerkleTree(const std::vector<std::string>& data) {
    if (data.empty()) {
        throw std::invalid_argument("Podaci za Merkle stablo ne smeju biti prazni.");
    }

    // Generisanje listova - heš vrednosti ulaznih podataka
    for (const auto& value : data) {
        leaves.push_back(hash(value));
    }

    // Gradimo stablo i dobijamo root hash
    rootHash = buildTree(leaves);
}


std::string MerkleTree::getRootHash() const {
	return rootHash;
}

std::string MerkleTree::hash(const std::string& data){
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(data.c_str()), data.size(), hash);

    std::ostringstream oss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }
    return oss.str();
}

std::string MerkleTree::buildTree(const std::vector<std::string>& hashes) {
    treeLevels.clear();
    if (hashes.empty()) return "";

    treeLevels.push_back(hashes);

    const auto* currentLevel = &hashes;
    while (currentLevel->size() > 1) {
        std::vector<std::string> nextLevel;

        for (size_t i = 0; i < currentLevel->size(); i += 2) {
            std::string left = (*currentLevel)[i];
            std::string right = (i + 1 < currentLevel->size()) ? (*currentLevel)[i + 1] : left;

            nextLevel.push_back(hash(left + right)); // Spajanje i heširanje
        }
        treeLevels.push_back(nextLevel);
        currentLevel = &treeLevels.back();
    }

    return currentLevel->front();
}


std::vector<std::pair<std::string, bool>> MerkleTree::generateProof(const std::string& data) const {
    std::vector<std::pair<std::string, bool>> proof;

    // Pronađi indeks podatka u listovima
    auto hashedData = hash(data);
    auto it = std::find(leaves.begin(), leaves.end(), hashedData);
    if (it == leaves.end()) {
        throw std::invalid_argument("Podatak nije pronadjen u Merkle stablu.");
    }

    size_t index = std::distance(leaves.begin(), it);

    // Kreni od nivoa listova do korena
    for (size_t level = 0; level < treeLevels.size() - 1; ++level) {
        const auto& currentLevel = treeLevels[level];
        bool isRight = (index % 2 == 1); // Da li je desni list
        size_t siblingIndex = isRight ? index - 1 : index + 1;

        if (siblingIndex < currentLevel.size()) {
            proof.push_back({ currentLevel[siblingIndex], isRight });
        }

        // Prelazimo na sledeći nivo
        index /= 2;
    }

    return proof;
}

bool MerkleTree::verifyProof(const std::string& rootHash, const std::string& data,
    const std::vector<std::pair<std::string, bool>>& proof) {
    std::string computedHash = hash(data); // Koristi istu hash metodu

    for (const auto& [siblingHash, isRight] : proof) {
        if (isRight) {
            computedHash = hash(siblingHash + computedHash); // Heširaj spajanje desno
        }
        else {
            computedHash = hash(computedHash + siblingHash); // Heširaj spajanje levo
        }
    }

    return computedHash == rootHash;
}
