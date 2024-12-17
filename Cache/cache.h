#pragma once
#include <string>
#include <unordered_map>

using namespace std;

typedef unsigned char byte;
typedef pair<int, string> composite_key;

struct Block {
	composite_key key;
	byte* data;

	Block();
	Block(composite_key k);
	~Block();

	void set_data(byte* data);

	bool operator == (const Block& b);
	byte operator[] (const int i);
	Block(const Block& other);
};
struct Node{
	Block block;
	Node* next;
	Node* prev;
	Node(Block b);
};

struct hash_composite_key {
	template <typename T1, typename T2>
	std::size_t operator ()(const std::pair<T1, T2>& p) const {
		auto h1 = std::hash<T1>{}(p.first);   // Hash of the int (first element of the pair)
		auto h2 = std::hash<T2>{}(p.second);  // Hash of the string (second element of the pair)

		// Combine the two hashes using XOR and bit shifting
		return h1 ^ (h2 << 1);
	}
};

class Cache {
	unordered_map<composite_key, Block, hash_composite_key> blocks;
	Node* head;
	Node* tail;
	int size;
	void add_node(Node* n);

public:
	void print_cache();
	Cache();
	bool get_block(const composite_key key, Block& b);
	void add_block(Block& b);
};
