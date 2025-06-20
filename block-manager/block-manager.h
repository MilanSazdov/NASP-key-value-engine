#pragma once
#include "cache.h"
#include <vector>
#include <string>
using namespace std;

const int block_size = 80;	    //in bytes

struct pair_hash {
	size_t operator()(const pair<int, string>& p) const {
		return hash<int>()(p.first) ^ hash<string>()(p.second);
	}
};

class Block_manager {
	Cache<composite_key, pair_hash> c;
public:
	void write_block(composite_key key, vector<byte> data);
	void write_block(composite_key key, string data);

	vector<byte> read_block(composite_key key, bool& error);
};