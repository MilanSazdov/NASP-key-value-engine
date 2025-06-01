#include <string>
#include <iostream>
#include <chrono>
#include <vector>
#include <map>
#include <fstream>
#include <filesystem>

#include "wal.h"
#include "block-manager.h"

namespace fs = std::filesystem;
using namespace std;

/*
	4 bytova crc32 hash sum
	1 byte flag
	8 bytova timestamp (vreme u sekundama kad je upisan record)
	1 byte tombstone (0 ako nije obrisan, 1 ako jeste)
	8 byta keysize
	8 byta datasize
	keysize bytova key
	datasize bytova data
*/

/*
	sta treba update? Citanje i pisanje preko block managera.
	--------- OVO BI TREBALO DA JE ODRADJENO ----------------------

*/

uint crc32_table[256];

inline int byte_to_int(std::byte b) {
	return static_cast<int>(b);
}

inline std::byte int_to_byte(int n) {
	return static_cast<std::byte>(n);
}

void init_crc32_table() {
	uint crc;
	for (int i = 0; i < 256; i++) {
		crc = i;
		for (int j = 8; j > 0; j--) {
			if (crc & 1) {
				crc = (crc >> 1) ^ 0xedb88320;
			}
			else {
				crc >>= 1;
			}
		}
		crc32_table[i] = crc;
	}
}

void Wal::ensure_wal_folder_exists() {
	if (!fs::exists("../wal/wal_logs")) {
		if (fs::create_directory("../wal/wal_logs")) {
			std::cout << "[WAL] Created folder: wal_logs\n";
		}
		else {
			std::cerr << "[WAL] ERROR: Could not create folder: wal_logs\n";
			throw std::runtime_error("Failed to create WAL folder.");
		}
	}
}

Wal::Wal() : segment_size(12), min_segment("../wal/wal_logs/wal_001.log") {
	ensure_wal_folder_exists();
	init_crc32_table();
	update_min_segment();
}

Wal::Wal(int segment_size) : segment_size(segment_size), min_segment("../wal/wal_logs/wal_001.log") {
	ensure_wal_folder_exists();
	init_crc32_table();
	update_min_segment();
};

uint crc32(byte* data, size_t length) {
	uint crc = 0xffffffff;  // Initial value
	for (size_t i = 0; i < length; i++) {
		byte table_index = int_to_byte((crc ^ byte_to_int(data[i])) & 0xff);
		crc = (crc >> 8) ^ crc32_table[byte_to_int(table_index)];
	}
	return (crc ^ 0xffffffff);
}

ull get_timestamp() {
	auto now = chrono::system_clock::now();

	auto duration_since_epoch = now.time_since_epoch();

	auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration_since_epoch);

	return (ull)seconds.count();
}

uint calc_crc(byte flag, ull timestamp, byte tombstone, ull key_size, ull value_size, string key, string value) {
	vector<byte> data_array;
	data_array.clear();
	data_array.push_back(flag);

	for (int i = 7; i >= 0; i--)
		data_array.push_back(int_to_byte((timestamp >> (i * 8)) & 255));	//TIMESTAMP

	data_array.push_back(tombstone);										//TOMBSTONE

	for (int i = 7; i >= 0; i--)
		data_array.push_back(int_to_byte((key_size >> (i * 8)) & 255));		//KEYSIZE

	for (int i = 7; i >= 0; i--)
		data_array.push_back(int_to_byte((value_size >> (i * 8)) & 255));	//DATASIZE

	for (char c : key)
		data_array.push_back(int_to_byte(c));								//KEY

	for (char c : value)
		data_array.push_back(int_to_byte(c));								//VALUE

	return crc32(data_array.data(), data_array.size());
}

uint char_to_uint(char* c) {
	uint ret = 0;
	int broj;

	for (int i = 3; i >= 0; i--) {
		broj = ((int)(byte)c[i]);
		ret += (broj << ((3 - i) * 8));
	}
	return ret;
}

ull char_to_ull(char* c) {
	ull ret = 0;
	ll broj;

	for (ll i = 7; i >= 0; i--) {
		broj = ((ll)(byte)c[i]);
		ret += (broj << ((7 - i) * 8));
	}
	return ret;
}

ull byte_to_ull(byte* c) {
	ull ret = 0;
	ll broj;
	for (ll i = 7; i >= 0; i--) {
		broj = ((ll)c[i]);
		ret += (broj << ((7 - i) * 8));
	}
	return ret;
}

ull byte_to_uint(byte* c) {
	uint ret = 0;
	int broj;
	for (ll i = 3; i >= 0; i--) {
		broj = ((ll)c[i]);
		ret += (broj << ((7 - i) * 8));
	}
	return ret;
}

int stoint(string& s) {
	int ten = 1;
	int ret = 0;
	for (int i = 2; i >= 0; i--) {
		ret += ten * (s[i] - '0');
		ten *= 10;
	}
	return ret;
}

string inttos3(int& x) {
	string ret = "000";
	for (int i = 0; i < 3; i++) {
		ret[2 - i] = (char)(x % 10 + '0');
		x /= 10;
	}
	return ret;
}

void fill_in_padding(vector<byte>& bad_data) {
	int n = (int)bad_data.size();
	int padding = block_size - (n % block_size);
	//cout << "Padding: " << padding << endl;
	while (padding) {
		bad_data.push_back(padding_character);
		padding--;
	}
}

composite_key next_key(composite_key key, int segment_size) {
	int id = key.first;
	string name = key.second;
	composite_key ret;
	if ((id + 1) < segment_size) {
		id++;
		//cout << "  id == " << id << endl;
		ret = make_pair(id, name);
	}
	else {
		string file_name = key.second;
		string broj = "";
		for (char c : file_name) {
			if (c >= '0' && c <= '9') broj.push_back(c);
		}
		int broj_int = stoint(broj);
		//cout << "novi brojh == " << broj_int << endl;
		broj_int++;
		string new_name = "../wal/wal_logs/wal_" + inttos3(broj_int) + ".log";

		//cout << new_name << endl;

		ret = make_pair(0, new_name);
	}
	return ret;
}

void debugf(Block b) {
	cout << "key: " << b.key.first << " " << b.key.second << endl;
	cout << "data:";
	for (int i = 0; i < block_size; i++) cout << byte_to_int(b[i]);
	cout << "kraj\n";
}

Block Wal::find_next_empty_block(Block b) {
	bool error;
	int pok = 0;
	Block b2 = b;

	do {
		pok++;
		//cout << "Checking block: " << b2.key.second << ", key: " << b2.key.first << endl;

		b2 = bm.read_block(b2.key, error);
		if (error) {
			// Create new file
			ofstream new_file(b2.key.second, ios::binary | ios::out);
			if (!new_file.is_open()) {
				cout << "Failed to create file: " << b2.key.second << endl;
				exit(1);
			}

			// Initialize the block with padding characters
			memset(b2.data, int_padding_character, block_size);
			bm.write_block(b2); // Write the initialized block

			return b2;
		}

		if (b2[block_size - 32] == padding_character &&
			b2[block_size - 31] == padding_character &&
			b2[block_size - 30] == padding_character)
			return b2;

		b2.key = next_key(b2.key, segment_size);

	} while (1);
}

int find_empty_pos(Block& b) {
	int poc = 0, key_size = 0, value_size = 0, sum;
	while (poc + 30 < block_size) {
		//cout << " poc == " << poc << endl;
		if (b[poc] == padding_character && b[poc + 1] == padding_character && b[poc + 2] == padding_character) return poc;

		key_size = byte_to_ull(b.data + 14 + poc);

		value_size = byte_to_ull(b.data + 22 + poc);

		//cout << key_size << " " << value_size << endl;
		sum = 30 + key_size + value_size;
		poc += sum;
	}

	return poc;
}

void extract_data(vector<byte>& record, uint crc, byte flag, ull timestamp, byte tombstone, ull key_size, ull value_size, string key, string value) {
	record.clear();
	for (int i = 3; i >= 0; i--)
		record.push_back(int_to_byte((crc >> (i * 8)) & 255));				///CRC

	record.push_back(flag);													///FLAG

	for (int i = 7; i >= 0; i--)
		record.push_back(int_to_byte((timestamp >> (i * 8)) & 255));		//TIMESTAMP

	record.push_back(tombstone);											//TOMBSTONE

	for (int i = 7; i >= 0; i--)
	record.push_back(int_to_byte((key_size >> (i * 8)) & 255));				//KEYSIZE

	for (int i = 7; i >= 0; i--)
		record.push_back(int_to_byte((value_size >> (i * 8)) & 255));		//VALUESIZE

	//cout << " extractng key: " << key << endl;
	for (char c : key)
		record.push_back(int_to_byte(static_cast<unsigned char>(c)));		//KEY

	//cout << " extractng value: " << value << endl;			
	for (char c : value)
		record.push_back(int_to_byte(static_cast<unsigned char>(c)));		//VALUE
	//fill_in_padding(record);
	//cout << " size == " << record.size() << endl;
}

void Wal::write_record(string key, string value, byte tombstone) {
	string current_file = min_segment;
	Block first_block(make_pair(0, current_file));
	Block b;

	b = find_next_empty_block(first_block);
	//debugf(b);
	//cout << endl;
	byte flag = (byte)0;
	ull timestamp = get_timestamp();
	ull key_size = key.size();
	ull value_size = value.size();

	uint crc = calc_crc(flag, timestamp, tombstone, key_size, value_size, key, value);

	vector<byte> record;

	int id = find_empty_pos(b);

	//no space for header at least
	if (block_size - id <= 30) {
		//cout << "Nema mesta za header\n";
		b.key = next_key(b.key, segment_size);
		memset(b.data, int_padding_character, block_size);
		id = 0;
	}

	int left_space;
	string key2, value2;

	if (key.size() + value.size() + 30 + id <= block_size) {
		//cout << key_size << " " << value_size << endl;
		extract_data(record, crc, flag, timestamp, tombstone, key_size, value_size, key, value);
		memcpy(&b.data[id], &record[0], record.size());
		
		bm.write_block(b);
	}
	else {
		flag = (byte)'F';
		//fragmentation
		while (key.size() + value.size() > 0) {
			//cout << "\n" << key << endl;
			//cout << value << endl;
			left_space = block_size - id;
			left_space -= 30;

			//cout << "LEFT SPACE: " << left_space << endl;
			if (key.size() > left_space) {
				key2 = key.substr(0, left_space);
				key = key.substr(left_space);
				value2 = "";
				left_space = 0;
			}
			else {
				key2 = key;
				key = "";
				//cout << " -= " << key2.size() << endl;
				left_space -= key2.size();
				if (value.size() > left_space) {
					value2 = value.substr(0, left_space);
					value = value.substr(left_space);
					left_space = 0;
				}
				else {
					value2 = value;
					value = "";
					flag = (byte)'L';
					left_space -= value2.size();
					//cout << " -= " << value2.size() << endl;
				}
			}
			//cout << "noviteti:\n";
			//cout << key2 << endl;
			//cout << value2 << endl;

			timestamp = get_timestamp();
			key_size = key2.size();
			value_size = value2.size();

			crc = calc_crc(flag, timestamp, tombstone, key_size, value_size, key2, value2);
			extract_data(record, crc, flag, timestamp, tombstone, key_size, value_size, key2, value2);

			memcpy(&b.data[id], &record[0], record.size());

			bm.write_block(b);

			b.key = next_key(b.key, segment_size);
			memset(b.data, int_padding_character, block_size);
			flag = (byte)'M';
			id = 0;
		}
	}
}

void debug_record(Record r) {
	cout << "CRC: " << r.crc << endl;
	cout << "FLAG: " << byte_to_int(r.flag) << endl;
	cout << "TIMESTAMP: " << r.timestamp << endl;
	cout << "TOMBSTONE: " << byte_to_int(r.tombstone) << endl;
	cout << "KEYSIZE: " << r.key_size << endl;
	cout << "VALUESIZE: " << r.value_size << endl;
	cout << "KEY: " << r.key << endl;
	cout << "VALUE: " << r.value << endl;
}

//valid: 0 bad record, 1 valid record, 2 no record(end)
Record read_record(Block b, int& pos, int& valid) {
	Record r;
	if (pos + 30 >= block_size) {
		valid = 2;
		return r;
	}
	if (b[pos] == padding_character && b[pos + 1] == padding_character && b[pos + 2] == padding_character) {
		valid = 2;
		return r;
	}

	r.crc = byte_to_uint(b.data + pos);
	r.flag = b[4 + pos];
	r.timestamp = byte_to_ull(b.data + 5 + pos);
	r.tombstone = b[13 + pos];
	r.key_size = byte_to_ull(b.data + 14 + pos);
	r.value_size = byte_to_ull(b.data + 22 + pos);

	r.key.resize(r.key_size);
	r.value.resize(r.value_size);

	memcpy(&r.key[0], b.data + 30 + pos, r.key_size);  // Copy the data
	memcpy(&r.value[0], b.data + 30 + r.key_size + pos, r.value_size);  // Copy the data

	valid = 1;
	if (r.crc != calc_crc(r.flag, r.timestamp, r.tombstone, r.key_size, r.value_size, r.key, r.value))
		valid = 0;

	pos += 30 + r.value_size + r.key_size;

	return r; 
}

void Wal::put(string key, string data) {
	//cout << "Putting (" << key << ", " << data << ") into wal\n";
	write_record(key, data);
}

bool validate_record_crc(Record r) {
	uint crc1 = calc_crc(r.flag, r.timestamp, r.tombstone, r.key_size, r.value_size, r.key, r.value);
	return (crc1 == r.crc);
}

void Wal::del(string key) {
	//cout << "Deleting " << key << " from wal\n";
	write_record(key, "", int_to_byte(1));
}

void Wal::update_min_segment() {
	min_segment = find_min_segment();
}

string Wal::find_min_segment() {
	vector<string> segment_files;
	int min_index = 999;

	for (const auto& entry : fs::directory_iterator("../wal/wal_logs")) {
		string filename = entry.path().filename().string();

		if (filename.substr(0, 4) == "wal_" && filename.substr(filename.length() - 4) == ".log") {
			string index_str = filename.substr(4, 3);
			int index = stoint(index_str);

			if (index < min_index) {
				min_index = index;
			}
		}
	}
	if (min_index == 999) {
		min_index = 1;
	}
	string min_index_str = inttos3(min_index);
	return "../wal/wal_logs/wal_" + min_index_str + ".log";
}

void Wal::delete_old_logs(string target_file) {
	/**
	 * Deletes WAL (Write-Ahead Logging) files in the "wal_logs" directory
	 * with an index smaller than the target file's index.
	 *
	 * @param target_file Name of the target WAL file (e.g., "wal_005.log").
	 *
	 * Extracts the index from the target file, identifies older files
	 * (e.g., "wal_001.log"), and deletes them. Logs success or error for each deletion.
	 */
	string index_str = target_file.substr(4, 3);
	int target_index = stoint(index_str);

	vector<string> files_to_delete;

	for (const auto& entry : fs::directory_iterator("../wal/wal_logs")) {
		string filename = entry.path().filename().string();

		if (filename.substr(0, 4) == "wal_" && filename.substr(filename.length() - 4) == ".log") {
			string current_index_str = filename.substr(4, 3);
			int current_index = stoint(current_index_str);

			if (current_index < target_index) {
				files_to_delete.push_back("../wal/wal_logs/" + filename);
			}
		}
	}

	for (const string& file : files_to_delete) {
		if (remove(file.c_str()) != 0) {
			cerr << "Error deleting file: " << file << endl;
		}
		else {
			cout << "Successfully deleted: " << file << endl;
		}
	}
	update_min_segment();
}

vector<Record> Wal::get_all_records() {
	vector<Record> ret;
	Record r;
	map<string, int> map_deleted;
	map<string, bool> already_in;

	bool er;
	int rec_pos, valid;

	composite_key tren_key(0, min_segment);
	Block b(tren_key);

	string big_key, big_value;
	bool all_good;

	while (1) {
		//getting block
		b = bm.read_block(b.key, er);
		//cout << "Reding from block:\n";
		//debugf(b);
		if (er) break;
		rec_pos = 0;
		//extracting records from block
		while (1) {
			r = read_record(b, rec_pos, valid);
			if (valid == 2)
				break;
			all_good = 1;
			if (r.flag == (byte)'F') {
				big_key = "";
				big_value = "";
				all_good &= valid;
				if (r.key_size) big_key += r.key;
				if (r.value_size) big_value += r.value;

				while (1) {
					r = read_record(b, rec_pos, valid);
					if (valid == 2) {
						b.key = next_key(b.key, segment_size);
						b = bm.read_block(b.key, er);
						rec_pos = 0;
						continue;
					}
					all_good &= valid;
					if (r.key_size) big_key += r.key;
					if (r.value_size) big_value += r.value;

					if (r.flag == (byte)'L') break;
				}
				if (!all_good) continue;
				r.key = big_key;
				r.value = big_value;

				r.key_size = big_key.size();
				r.value_size = big_value.size();

				ret.push_back(r);
				map_deleted[r.key]++;
			}
			else {
				map_deleted[r.key]++;
				ret.push_back(r);
			}
		}

		b.key = next_key(b.key, segment_size);
	}

	vector<Record> ret2;
	ret2.clear();

	//cout << "KRAJ\n";
	for (int i = 0; i < ret.size(); i++) {
		if (map_deleted[ret[i].key] % 2 == 0) {
			//cout << "Erasing\n";
		}
		else {
			if (already_in.find(ret[i].key) != already_in.end()) continue;
			ret2.push_back(ret[i]);
			//debug_record(ret[i]);
			already_in[ret[i].key] = 1;
			//cout << endl;
		}
	}
	return ret2;
}

/*string Wal::get(string key) {
	string ret="";
	Record r;
	bool succes;

	ifstream file("wal_logs/wal_001.log", ios::in | ios::binary);
	if (file.is_open()) {
		while (file) {
			succes = read_record(file, r);
			if (!succes) break;
			if (!validate_record_crc(r)) continue;
			if (r.key == key) {
				//debug_record(r);
				if (r.tombstone == 1) ret = "";
				else ret = r.value;
			}
		}
	}
	else cout << "Error while opening file\n";

	file.close();
	return ret;
}*/