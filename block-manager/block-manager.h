#pragma once
#include "cache.h"
#include <vector>
#include <string>
using namespace std;

//typedef pair<int, string> composite_key;
string make_string_from_pair(int x, string s);
pair<int, string> make_pair_from_string(string s);

class Block_manager {
	Cache c;
public:
	void write_data(string file_name, vector<char> data);
	
	vector<char> read_data(string file_name);			/// MISLIM DA OVO NE SME DA POSTOJI !!!

	Block read_block(string key, bool& error);
	Block read_block(int id, string file_name, bool& error);	 //same as above

	//void write_block(composite_key key, vector<char> data);
	void write_block(Block b);										//update of block
	//void write_block(int id, string file_name, vector<char> data);	//same as above

	//void write_new_block(string file_name, vector<char> data);	//append data, it can be more than one block
};