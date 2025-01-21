#include "block-manager.h"
#include "cache.h"
#include <fstream>
#include <iostream>

string make_string_from_pair(int x, string s) {
	string poc = to_string(x);
	return (poc + "_" + s);
}

pair<int, string> make_pair_from_string(string s) {
	pair<int, string> ret;
	string str = "";
	for (char c : s) {
		if (c == '_') break;
		str.push_back(c);
	}

	ret.first = stoi(str);
	ret.second = s.substr(str.size()+1);

	//cout << ret.first << " " << ret.second << endl;

	return ret;
}

Block Block_manager::read_block(string key_string, bool& error) {
	bool exists;
	Block my_block(key_string);
	error = false;
	exists = c.get_block(key_string, my_block);

	pair<int, string> key;
	key = make_pair_from_string(key_string);


	if (!exists) {
		//cout << "It exists";
		ifstream file(key.second, ios::in | ios::binary);
		if (file.is_open()) {
			//cout << "File is open";
			file.seekg(0, std::ios::end);
			streampos last_pos = file.tellg();

			file.seekg(block_size*key.first, ios::beg);

			if (file.tellg() == last_pos) {	//end of file
				error = false;
				memset(my_block.data, int_padding_character, block_size);
				c.add_block(my_block);
				file.close();
				return my_block;
			}

			char* buffer = new char[block_size];
			if (!file.read(buffer, block_size)) {	//unsuccesful read
				error = true;
				//throw("Unsuccesful read block from file " + key.second);
				file.close();
				return my_block;
			}
			
			my_block.set_data(buffer);
			c.add_block(my_block);
			delete[] buffer;
		}
		else {
			error = true;
			memset(my_block.data, (int)padding_character, block_size);
			//cout << "File is not open";
			//throw("Error opening file: " + key.second);
			file.close();
			return my_block;
		}

		file.close();
	}
	error = false;
	return my_block;
}

Block Block_manager::read_block(int id, string file_name, bool& error) {
	return read_block(make_string_from_pair(id, file_name), error);
}

void Block_manager::write_block(Block b) {
	pair<int, string> key;
	key = make_pair_from_string(b.key);

	ofstream out_file(key.second, ios::in | ios::out | ios::binary);
	if (!out_file.is_open()) {
		// If file doesnt exist create it
		out_file.open(key.second, ios::binary | ios::out);
		if (!out_file.is_open()) {
			throw("Failed to create file: " + key.second);
		}
	}
	int new_pos = key.first * block_size;
	//cout << "New pos: " << new_pos << endl;
	out_file.seekp(new_pos);
	out_file.write(reinterpret_cast<const char*>(b.data), block_size);
	out_file.close();

	c.add_block(b);
}

void fill_in_padding(vector<char>& bad_data) {
	int n = bad_data.size();
	int padding = block_size - (n % block_size);
	while(padding){
		bad_data.push_back((int)padding_character);
		padding--;
	}
}

void Block_manager::write_data(string file_name, vector<char> data) {
	int n = data.size(), last = 0, end = block_size;
	if (n < block_size) {
		//cout << "n < block_size\n";
		fill_in_padding(data);
		//for (int i = 0; i < block_size; i++)
		//	cout << data[i];
		//cout << endl;

		Block b(make_string_from_pair(0, file_name));

		b.set_data(&data[0]);
		//for (int i = 0; i < block_size; i++)
		//	cout << b[i];
		//cout << endl;

		write_block(b);
		return;
	}
	for (int i = block_size-1; i < n; i += block_size) {
		Block b(make_string_from_pair(last, file_name));
		b.set_data(&data[i - block_size+1]);
		write_block(b);

		last++;
	}
	last++;
	if (last * block_size == n) return;

	fill_in_padding(data);
	Block b_end(make_string_from_pair(last, file_name));
	n = data.size();
	b_end.set_data(&data[n - block_size]);
	write_block(b_end);
}

vector<char> Block_manager::read_data(string file_name) {
	vector<char> ret;
	bool error = false;
	int id = 0;
	while (1) {
		Block b = read_block(id, file_name, error);
		if (error) break;
		for (int i = 0; i < block_size; i++)
			ret.push_back((char)b[i]);
		id++;
	}

	return ret;
}