#pragma once
#include "cache.h"
#include <vector>
#include <string>
#include "Config.h"
using namespace std;

struct pair_hash {
	size_t operator()(const pair<int, string>& p) const {
		return hash<int>()(p.first) ^ hash<string>()(p.second);
	}
};

class Block_manager {
	int block_size;
	Cache<composite_key, pair_hash>* c;

	void fill_in_padding(vector<byte>& bad_data);

public:
	Block_manager();
	void write_block(composite_key key, vector<byte> data);
	void write_block(composite_key key, string data);

	vector<byte> read_block(composite_key key, bool& error);
};