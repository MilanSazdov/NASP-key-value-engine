#pragma once
#include "cache.h"
#include <vector>
#include <string>
using namespace std;

//typedef pair<int, string> composite_key;

class Block_manager {
	Cache c;
public:
	//Block_manager();
	void write_data(string file_name, vector<char> data);
	vector<char> read_data(string file_name);

	Block read_block(composite_key key, bool& error);
	Block read_block(int id, string file_name, bool& error);	 //same as above

	//void write_block(composite_key key, vector<char> data);
	void write_block(Block b);										//update of block
	//void write_block(int id, string file_name, vector<char> data);	//same as above

	//void write_new_block(string file_name, vector<char> data);	//append data, it can be more than one block
};