#include "block-manager.h"
#include "cache.h"
#include <fstream>
#include <iostream>
#include <algorithm>

void fill_in_padding(vector<byte>& bad_data) {
	cout << "Filling padding\n";
	int n = bad_data.size();
	if (n == block_size) return;

	int padding = block_size - (n % block_size);
	while(padding){
		bad_data.push_back((byte)padding_character);
		padding--;
	}
}

void Block_manager::write_block(composite_key key, vector<byte> data) {
	ofstream out_file(key.second, ios::in | ios::out | ios::binary);
	
	// If file doesnt exist create it
	if (!out_file.is_open()) {
		out_file.open(key.second, ios::binary | ios::out);
		if (!out_file.is_open()) {
			throw("Failed to create file: " + key.second);
		}
	}
	int new_pos = key.first * block_size;
	
	fill_in_padding(data);

	out_file.seekp(new_pos);
	cout << "writing: ";
	for (byte x : data) cout << (char)x;
	cout << endl;

	out_file.write(reinterpret_cast<const char*>(data.data()), block_size);
	out_file.close();

	c.put(key, data);
}

void Block_manager::write_block(composite_key key, string data){
	vector<byte> bytes;

	std::transform(data.begin(), data.end(), bytes.begin(),
			[] (char c) { return std::byte(c); });

	write_block(key, bytes);

}

vector<byte> Block_manager::read_block(composite_key key, bool& error) {
	error = false;
	//cout << "Reading block: " << key.first << " " << key.second << endl;

	bool exists = false;
	vector<byte> ret;

	ret = c.get(key, exists);

	if (exists) {
		return ret;
	}
	
	ifstream file(key.second, ios::in | ios::binary);

	if (file.is_open()) {
		// calculating last position
		file.seekg(0, ios::end);
		streampos last_pos = file.tellg();

		file.seekg(block_size * key.first, ios::beg);

		if (file.tellg() >= last_pos) {	//reached end of file
			error = true;
		}
		else {
			cout << "Uspesno citam\n";
			char* buffer = new char[block_size];
			// unsuccesful read
			if (!file.read(buffer, block_size)) {
				error = true;
			}
			else {
				const byte* first = reinterpret_cast<const byte*>(buffer);
				const byte* last = reinterpret_cast<const byte*>(buffer + block_size);

				// Assign the contents of the buffer to the vector
				ret.assign(first, last);

				c.put(key, ret);
				delete[] buffer;
			}
		}
	}

	// could not open file
	else {
		error = true;
	}

	file.close();
	return ret;
}