#include "MerkleTree.h"
#include <iostream>

int main() {
    std::vector<std::string> data = { "Podatak1", "Podatak2", "Podatak3", "Podatak4" };

    try {
        MerkleTree tree(data);
        std::cout << "Root Hash: " << tree.getRootHash() << std::endl;

        // Generisanje dokaza za "Podatak2"
        std::string target = "Podatak2";
        auto proof = tree.generateProof(target);

        // Provera dokaza
        bool isValid = MerkleTree::verifyProof(tree.getRootHash(), target, proof);
        std::cout << "Provera dokaza za '" << target << "': " << (isValid ? "Validno" : "Nevalidno") << std::endl;

    }
    catch (const std::exception& e) {
        std::cerr << "Greška: " << e.what() << std::endl;
    }

    return 0;
}
