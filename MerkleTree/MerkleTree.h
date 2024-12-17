#pragma once

#include <vector>
#include <string>
#include <functional>

using namespace std;

class MerkleTree {

private:
	
	vector<string> leaves; // Hashes of the leaves
	string rootHash; // Hash of the root

	string hash(const string& data) const;
	string buildTree(vector<string> hashes);


public:

	MerkleTree(const vector<string>& data);
	string getRootHash() const;

};