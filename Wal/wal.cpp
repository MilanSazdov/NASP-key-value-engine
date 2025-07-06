#include <string>
#include <iostream>
#include <chrono>
#include <vector>
#include <map>
#include <fstream>
#include <filesystem>
#include <cstring>

#include "wal.h"

using namespace std;
namespace fs = filesystem;

/*
	4 bytova crc32 hash sum
	1 byte flag	(used for fracttioning user input)
	8 bytova timestamp (vreme u sekundama kad je upisan record)
	1 byte tombstone (0 ako nije obrisan, 1 ako jeste)
	8 byta keysize
	8 byta datasize
	keysize bytova key
	datasize bytova data
*/
composite_key Wal::current_block;

void debug_bytes(vector<byte> by) {
	cout << "Read value:";
	for (byte b : by) {
		cout << (char)b;
	}
	cout << "kraj\n";
}

void debug_record(Record r) {
	cout << "CRC: " << r.crc << endl;
	cout << "TIMESTAMP: " << r.timestamp << endl;
	cout << "TOMBSTONE: " << int(r.tombstone) << endl;
	cout << "KEYSIZE: " << r.key_size << endl;
	cout << "VALUESIZE: " << r.value_size << endl;
	cout << "KEY: " << r.key << endl;
	cout << "VALUE: " << r.value << endl;
}

uint crc32_table[256];
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

string inttos3(int x) {
	string ret = "000";
	for (int i = 0; i < 3; i++) {
		ret[2 - i] = (char)(x % 10 + '0');
		x /= 10;
	}
	return ret;
}

string find_min_segment(const string& log_directory) {
	int min_index = 999;

	for (const auto& entry : fs::directory_iterator(log_directory)) {
		string filename = entry.path().filename().string();

		if (filename.substr(0, 4) == "wal_" && filename.substr(filename.length() - 4) == ".log") {
			string index_str = filename.substr(4, 3);
			int index = stoi(index_str);

			if (index < min_index) {
				min_index = index;
			}
		}
	}

	if (min_index == 999) {
		min_index = 1;
	}

	string min_index_str = inttos3(min_index);

	return log_directory + "/wal_" + min_index_str + ".log";
}

void Wal::ensure_wal_folder_exists() {
	if (!fs::exists(log_directory)) {
		if (fs::create_directory(log_directory)) {
			std::cout << "[WAL] Created folder: " << log_directory << "\n";
		}
		else {
			std::cerr << "[WAL] ERROR: Could not create folder: " << log_directory << "\n";
			throw std::runtime_error("Failed to create WAL folder.");
		}
	}
}

void Wal::update_current_block() {
	int max_index = 0, index;
	string index_str, my_file_name;

	for (const auto& entry : fs::directory_iterator(log_directory)) {
		string file_name = entry.path().filename().string();

		if (file_name.substr(0, 4) == "wal_" && file_name.substr(file_name.length() - 4) == ".log") {
			index_str = file_name.substr(4, 3);
			index = stoi(index_str);

			if (index > max_index) {
				max_index = index;
				my_file_name = file_name;
			}
		}
	}
	cout << "max_index = " << max_index << endl;
	if (max_index == 0) {
		current_block = make_pair(0, log_directory + "/wal_001.log");
	}
	else {
		uintmax_t size = fs::file_size(log_directory + "/" + my_file_name);
		current_block = make_pair(max(0, (int)size / Config::block_size - 1), log_directory + "/" + my_file_name);
	}
}

Wal::Wal() : segment_size(Config::segment_size), log_directory(Config::wal_directory){
	ensure_wal_folder_exists();
	init_crc32_table();
	min_segment = find_min_segment(log_directory);
	update_current_block();
}

ull get_timestamp() {
	auto now = chrono::system_clock::now();

	auto duration_since_epoch = now.time_since_epoch();

	auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration_since_epoch);

	return (ull)seconds.count();
}

uint crc32(byte* data, size_t length) {
	uint crc = 0xffffffff;  // Initial value
	for (size_t i = 0; i < length; i++) {
		byte table_index = static_cast<byte>((crc ^ static_cast<int>(data[i])) & 0xff);
		crc = (crc >> 8) ^ crc32_table[static_cast<int>(table_index)];
	}
	return (crc ^ 0xffffffff);
}

uint calc_crc(Wal_record_type flag, ull timestamp, byte tombstone, ull key_size, ull value_size, string key, string value) {
	vector<byte> data_array;
	data_array.clear();
	data_array.push_back((byte)flag);											// FLAG (first, middle, last, full)

	for (int i = 7; i >= 0; i--)
		data_array.push_back(static_cast<byte>((timestamp >> (i * 8)) & 255));	//TIMESTAMP

	data_array.push_back(tombstone);										//TOMBSTONE

	for (int i = 7; i >= 0; i--)
		data_array.push_back(static_cast<byte>((key_size >> (i * 8)) & 255));		//KEYSIZE

	for (int i = 7; i >= 0; i--)
		data_array.push_back(static_cast<byte>((value_size >> (i * 8)) & 255));	//DATASIZE

	for (char c : key)
		data_array.push_back(static_cast<byte>(c));								//KEY

	for (char c : value)
		data_array.push_back(static_cast<byte>(c));								//VALUE

	return crc32(data_array.data(), data_array.size());
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

Wal_record_type byte_to_flag(byte* c) {
	Wal_record_type ret = ((Wal_record_type)c[0]);
	return ret;
}

ull find_empty_pos(vector<byte> b) {
	if (b.size() == 0) return 0;
	ull poc = 0, key_size = 0, value_size = 0, sum;
	bool empty;

	while (poc + 30 < Config::block_size) {
		empty = 1;
		for (int j = 0; j < 20; j++) {
			if (b[poc + j] != padding_character) {
				empty = 0;
				break;
			}
		}
		if (empty) return poc;

		key_size = byte_to_ull(b.data() + 14 + poc);

		value_size = byte_to_ull(b.data() + 22 + poc);

		sum = 30 + key_size + value_size;
		poc += sum;
	}

	return -1;
}

void Wal::next_block(composite_key& block) {
	if (block.first + 1 < segment_size) {
		block.first++;
	}
	else {
		//transfering wal_042.log -> int num = 42
		int key_size = block.second.size();
		int num = block.second[key_size -5] - '0' + (block.second[key_size -6] - '0') * 10 + (block.second[key_size-7] - '0') * 100;
		
		
		num++;
		string new_offset = inttos3(num);

		block.first = 0;
		
		block.second[key_size - 7] = new_offset[0];
		block.second[key_size - 6] = new_offset[1];
		block.second[key_size - 5] = new_offset[2];
	}
}

void extract_data(vector<byte>& record, uint crc, Wal_record_type flag, ull timestamp, byte tombstone, ull key_size, ull value_size, string key, string value) {
	for (int i = 3; i >= 0; i--)
		record.push_back((byte)((crc >> (i * 8)) & 255));				///CRC

	//cout << "Pre svega: " << (char)(flag) << endl;
	record.push_back((byte)flag);										///FLAG

	for (int i = 7; i >= 0; i--)
		record.push_back((byte)((timestamp >> (i * 8)) & 255));		//TIMESTAMP

	record.push_back(tombstone);											//TOMBSTONE

	for (int i = 7; i >= 0; i--)
		record.push_back((byte)((key_size >> (i * 8)) & 255));				//KEYSIZE

	for (int i = 7; i >= 0; i--)
		record.push_back((byte)((value_size >> (i * 8)) & 255));		//VALUESIZE

	//cout << " extractng key: " << key << endl;
	for (char c : key)
		record.push_back((byte)(static_cast<unsigned char>(c)));		//KEY

	//cout << " extractng value: " << value << endl;			
	for (char c : value)
		record.push_back((byte)(static_cast<unsigned char>(c)));		//VALUE
	//fill_in_padding(record);
	//cout << " size == " << record.size() << endl;
}

void Wal::write_record(string key, string value, byte tombstone) {
	bool error;
	vector<byte> bytes;
	//cout << current_block.first << " " << current_block.second << endl;
	
	bytes = bm.read_block(current_block, error);

	int pos = find_empty_pos(bytes);

	if (pos == -1) {
		next_block(current_block);
		pos = 0;
	}
	vector<byte> record;
	
	ull timestamp = get_timestamp();
	ull key_size = key.size();
	ull value_size = value.size();
	uint crc;
	Wal_record_type flag;

	// no fractioning
	if (key.size() + value.size() + 30 + pos <= Config::block_size) {		
		record.clear();
		for (int i = 0; i < pos; i++) {
			record.push_back(bytes[i]);
		}

		flag = Wal_record_type::FULL;
		crc = calc_crc(flag, timestamp, tombstone, key_size, value_size, key, value);
		extract_data(record, crc, flag, timestamp, tombstone, key_size, value_size, key, value);

		//cout << "writing done (FULL)\n";
		
		bm.write_block(current_block, record);
	}
	else {
		string key_new, value_new;
		record.clear();
		for (int i = 0; i < pos; i++) {
			record.push_back(bytes[i]);
		}
		int visak;
		flag = Wal_record_type::FIRST;

		while (key.size() + value.size() > 0) {
			visak = Config::block_size - 30 - pos;
			if (visak <= key.size()) {
				key_new = key.substr(0, visak);
				key = key.substr(visak);
				value_new = "";
			}
			else if (key.size() == 0) {
				key_new = "";
				if (visak <= value.size()) {
					value_new = value.substr(0, visak);
					value = value.substr(visak);
				}
				else {
					value_new = value;
					value = "";
				}
			}
			else {
				key_new = key;
				visak -= key.size();
				key = "";

				if (visak <= value.size()) {
					value_new = value.substr(0, visak);
					value = value.substr(visak);
				}
				else {
					value_new = value;
					value = "";
				}
			}

			if (key.size() + value.size() == 0) flag = Wal_record_type::LAST;

			key_size = key_new.size();
			value_size = value_new.size();
			timestamp = get_timestamp();

			crc = calc_crc(flag, timestamp, tombstone, key_size, value_size, key_new, value_new);
			extract_data(record, crc, flag, timestamp, tombstone, key_size, value_size, key_new, value_new);

			//cout << "writing done (" << record_type_to_string(flag) << ")\n";

			bm.write_block(current_block, record);
			
			pos = 0;
			next_block(current_block);

			flag = Wal_record_type::MIDDLE;
			record.clear();
		}
	}
	//cout << current_block.first << " " << current_block.second << " pos in block: " << pos << endl;
}

//valid: 0 bad record, 1 valid record, 2 no record(end)
Record read_record(vector<byte> b, int& pos, int& valid, Wal_record_type& flag) {
	Record r;
	if (pos + 30 >= Config::block_size) {
		valid = 2;
		return r;
	}

	if (b[pos] == padding_character && b[pos + 1] == padding_character && b[pos + 2] == padding_character) {
		valid = 2;
		return r;
	}

	r.crc = byte_to_uint(b.data() + pos);

	flag = byte_to_flag(b.data()+pos+4);	

	r.timestamp = byte_to_ull(b.data() + 5 + pos);
	r.tombstone = b[13 + pos];
	r.key_size = byte_to_ull(b.data() + 14 + pos);
	r.value_size = byte_to_ull(b.data() + 22 + pos);

	r.key.resize(r.key_size);
	r.value.resize(r.value_size);

	std::memcpy(&r.key[0], b.data() + 30 + pos, r.key_size);  // Copy the data
	std::memcpy(&r.value[0], b.data() + 30 + r.key_size + pos, r.value_size);  // Copy the data

	valid = 1;
	if (r.crc != calc_crc(flag, r.timestamp, r.tombstone, r.key_size, r.value_size, r.key, r.value))
		valid = 0;

	pos += 30 + r.value_size + r.key_size;

	return r;
}

vector<Record> Wal::get_all_records() {
	vector<Record> records;
	vector<Record> fragm;
	bool error = false;

	min_segment = find_min_segment(log_directory);
	composite_key key(0, min_segment);

	vector<byte> bytes;
	Wal_record_type flag;

	bytes = bm.read_block(key, error);
	//cout << "error = " << error << endl;
	string new_value, new_key;

	while (!error) {
		int pos = 0;
		//cout << "Current block : " << key.first << " " << key.second << endl;

		while (true) {
			int ok = 2;
			Record r = read_record(bytes, pos, ok, flag);

			if (ok == 2) break;         // Kraj bloka
			else if (ok == 0) {			// disk error, should NOT count this record
				if (flag == Wal_record_type::LAST) {
					fragm.clear();
				}
				continue;
			}
			// whole record completed, goes directly in ret
			if (flag == Wal_record_type::FULL) {
				records.push_back(r);
				fragm.clear();
				continue;
			}
			fragm.push_back(r);
			if(flag == Wal_record_type::LAST){
				new_key = new_value = "";
				for(Record r: fragm){
					new_key = new_key + r.key;
					new_value = new_value + r.value;
				}
				r.key = new_key;
				r.value = new_value;

				r.value_size = r.value.size();
				r.key_size = r.key.size();

				// r.crc and r.timestamp may be different
				records.push_back(r);
			}
			
			
			//debug_record(r);
			//cout << record_type_to_string(flag) << endl;
			//cout << endl;

			// Ako je CRC nevalidan (ok == 0), ignori�i samo taj record
		}

		next_block(key);
		bytes = bm.read_block(key, error);
	}

	return records;
}

void Wal::put(string key, string value){
	write_record(key, value, (byte)0);
}

void Wal::del(string key) {
	write_record(key, "", (byte)1);
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
	int target_index = stoi(index_str);

	vector<string> files_to_delete;

	for (const auto& entry : fs::directory_iterator(log_directory)) {
		string filename = entry.path().filename().string();

		if (filename.substr(0, 4) == "wal_" && filename.substr(filename.length() - 4) == ".log") {
			string current_index_str = filename.substr(4, 3);
			int current_index = stoi(current_index_str);

			if (current_index < target_index) {
				files_to_delete.push_back(log_directory + filename);
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
}