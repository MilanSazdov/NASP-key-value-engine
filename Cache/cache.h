#pragma once
#include <string>
#include <unordered_map>
#include <cstddef>

const int block_size = 100;	    //in bytes
const int cache_size = 5;		//in blocks

const std::byte padding_character = (std::byte)'0';
const int int_padding_character = (int)'0';

struct Block {
	std::string key;
	std::byte* data;

	Block();
	Block(std::string k);
	~Block();

	void set_data(char* data);

	bool operator == (const Block& b);
	std::byte operator[] (const int i);
	Block(const Block& other);				//copy constructor (deep copy)
	Block& operator=(const Block& other);	//asignment operator (deep copy)
};

struct Node{
	Block block;

	Node* next;
	Node* prev;
	Node(Block b);
};

class Cache {
	std::unordered_map<std::string, Block> blocks;
	Node* head;
	Node* tail;
	int size;
	void add_node(Node* n);

public:
	void print_cache();
	Cache();
	bool get_block(const std::string key, Block& b);
	void add_block(Block& b);
};
