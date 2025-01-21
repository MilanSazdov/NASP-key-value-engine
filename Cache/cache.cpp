#include "cache.h"
#include <string>
#include <iostream>
#include <cstddef>


Block::Block() {
	key = "";
	data = new std::byte[block_size];
	memset(data, (int)padding_character, block_size);
}

Block::Block(std::string k) : key(k) {
	this->data = new std::byte[block_size];
	memset(data, (int)padding_character, block_size);
}

Block::Block(const Block& other) : key(other.key) {
	this->data = new std::byte[block_size];
	memcpy(this->data, other.data, block_size); // Deep copy the data array
}

Block& Block::operator=(const Block& other) {
	if (this != &other) {
		key = other.key;
		memcpy(data, other.data, block_size);
	}
	return *this;
}

Block::~Block() {
	//cout << "Deleting: " << key.first << " " << key.second << endl;
	delete[] data;
}

void Block::set_data(char* data) {
	memcpy(this->data, data, block_size);
}

bool Block::operator == (const Block& b) {
	return (b.key == key);
}

std::byte Block::operator[] (const int i) {
	if (i < 0 || i >= block_size) {
		throw "Index out of bounds";
	}
	return data[i];
}

///____________________________________________________________________________________

void print_node(Node* n) {
	if (n == nullptr) {
		std::cout << "Node is nullptr\n";
		return;
	}
	std::cout << "Node: " << n->block.key << std::endl;
}

Node::Node(Block b) : block(b) {
	next = nullptr;
	prev = nullptr;
}
///____________________________________________________________________________________

Cache::Cache() {
	head = nullptr;
	tail = nullptr;
	size = cache_size;
}

void Cache::print_cache() {
	std::cout << "Printing cache\n";
	std::cout << "Map:\n";
	for (auto it = blocks.begin(); it != blocks.end(); it++) {
		std::cout << it->first << std::endl;
	}
	std::cout << "List:\n";
	Node* temp = tail;
	while (temp != nullptr) {
		std::cout << temp->block.key << std::endl;
		temp = temp->next;
	}
	std::cout << std::endl;
}

void Cache::add_node(Node* n) {
	//block already in cache, this is modifying
	if (blocks.find(n->block.key) != blocks.end()) {
		blocks[n->block.key] = n->block;

		if (head->block.key == n->block.key) {
			head->block = n->block;
			return;
		}
		
		Node* exit = head;
		while (exit != nullptr) {
			if (exit->block.key == n->block.key) {
				break;
			}
			exit = exit->prev;
		}
		//print_node(exit);

		Node* prev = exit->prev;
		Node* next = exit->next;

		next->prev = prev;
		if (prev != nullptr) {
			prev->next = next;
		}
		else {
			tail = next;
		}

		n->prev = head;
		n->next = nullptr;
		head->next = n;
		head = n;
		
		return;
	}
	//adding to map
	blocks[n->block.key] = n->block;

	//list is empty
	if (head == nullptr) {
		head = n;
		tail = n;
		//cout << "Oba nullptr\n";
		return;
	}
	//list is full
	if (blocks.size() > size) {
		//cout << "Erasing " << tail->block.key.first << " " << tail->block.key.second << endl;
		blocks.erase(tail->block.key);
		Node* old_tail = tail;
		tail = tail->next;
		tail->prev = nullptr;
	}
	//adding to the head
	head->next = n;
	n->prev = head;
	head = n;
}

bool Cache::get_block(const std::string key, Block& b){
	Block* temp = new Block();

	if (blocks.find(key) != blocks.end()) {
		temp->key = key;
		memcpy(temp->data, blocks[key].data, block_size);

		b = *temp;
		return true;
	}
	else return false;
}

void Cache::add_block(Block& b) {
	add_node(new Node(b));
}
