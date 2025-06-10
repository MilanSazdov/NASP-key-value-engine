// source: https://www.geeksforgeeks.org/lru-cache-implementation-using-double-linked-lists/Add commentMore actions
#pragma once
#pragma once
#include <string>
#include <unordered_map>
#include <cstddef>
#include <iostream>

using namespace std;
/*
typedef pair<int, string> composite_key;
const int block_size = 100;	    //in bytes

const std::byte padding_character = (std::byte)'0';
const int int_padding_character = (int)'0';*/

const int cache_capacity = 4;		//in blocks



template <typename key_type>
struct Node {
	key_type key;
	vector<byte> value;

	Node* next;
	Node* prev;

	Node() {
		next = nullptr;
		prev = nullptr;
	}
	Node(key_type key) : key(key) {
		next = nullptr;
		prev = nullptr;
	};

	Node(key_type key, vector<byte> value) : key(key), value(value) {
		next = nullptr;
		prev = nullptr;
	};
};

// 2 USE CASES:
// 
// Cache <string> c;
// Cache <composite_key, hash_function> c;



// FOR USAGE with COMPOSITE_KEY
// you must use hash function provided here

/*
struct pair_hash {
	size_t operator()(const pair<int, string>& p) const {
		return hash<int>()(p.first) ^ hash<string>()(p.second);
	}
};
*/

// AND THEN CALL IT LIKE THIS
/// Cache<composite_key, pair_hash> block_cache;

template <typename key_type, typename hash = hash<key_type>>

class Cache {
public:
	Node<key_type>* tail;
	Node<key_type>* head;

	int capacity;
	unordered_map<key_type, Node<key_type>*, hash> cache_map;

	/*void print_cache() {
		cout << " Cache (head -> tail) [" << cache_map.size()  << "]:\n";

		Node<key_type>* temp = head;
		while (temp != nullptr) {
			cout << temp->key << endl;
			temp = temp->next;
		}

		cout << "_________\n";
	}*/

	Cache() {
		head = nullptr;
		tail = nullptr;
		capacity = cache_capacity;
	}

	// function to remove node from cache
	void remove_node(Node<key_type>* node) {
		if (node == nullptr) return;

		Node<key_type>* prev = node->prev;
		Node<key_type>* next = node->next;

		// list has only one item
		if (prev == nullptr && next == nullptr) {
			head = tail = nullptr;
			return;
		}

		if (node == tail) {
			if (prev != nullptr) prev->next = nullptr;
			tail = prev;
		}
		else if (node == head) {
			if (next != nullptr) next->prev = nullptr;
			head = next;
		}
		else {
			if (prev != nullptr) prev->next = next;
			if (next != nullptr) next->prev = prev;
		}
	}

	// function to add Node to be head
	void add_node(Node<key_type>* node) {
		// list is empty
		if (head == nullptr) {
			head = tail = node;
			return;
		}
		Node<key_type>* old_head = head;

		head = node;
		head->prev = nullptr;
		head->next = old_head;

		old_head->prev = head;
	}

	// function to add key, value pair into cache
	void put(key_type key, vector<byte> value) {
		if (cache_map.find(key) != cache_map.end()) {
			Node<key_type>* old_node = cache_map[key];
			remove_node(old_node);
			delete old_node;
		}

		Node<key_type>* node = new Node<key_type>(key, value);
		cache_map[key] = node;
		add_node(node);

		if (cache_map.size() > capacity) {
			key_type key_to_delete = tail->key;

			// deleting tail
			remove_node(tail);
			cache_map.erase(key_to_delete);
		}

	}

	// function to get value. If error == true, key doesnt exists
	vector<byte> get(key_type key, bool& error) {
		// it doesnt exists
		if (cache_map.find(key) == cache_map.end()) {
			error = true;
			return vector<byte>();
		}

		Node<key_type>* ret = cache_map[key];

		// remove node from whetever node is
		remove_node(ret);

		// add node to head
		add_node(ret);

		error = false;
		return ret->value;
	}
};