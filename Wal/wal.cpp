#include <string>
#include <iostream>
#include <chrono>
#include <vector>
#include <map>

#include "wal.h"
#include "block-manager.h"
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

*/

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

Wal::Wal() : segment_size(12) {
	init_crc32_table();
};

Wal::Wal(int segment_size) : segment_size(segment_size) {
	init_crc32_table();
};

uint crc32(byte* data, size_t length) {
	uint crc = 0xffffffff;  // Initial value
	for (size_t i = 0; i < length; i++) {
		byte table_index = (crc ^ data[i]) & 0xff;
		crc = (crc >> 8) ^ crc32_table[table_index];
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
		data_array.push_back((timestamp >> (i * 8)) & 255);			//TIMESTAMP

	data_array.push_back(tombstone);								//TOMBSTONE

	for (int i = 7; i >= 0; i--)
		data_array.push_back((key_size >> (i * 8)) & 255);			//KEYSIZE

	for (int i = 7; i >= 0; i--)
		data_array.push_back((value_size >> (i * 8)) & 255);			//DATASIZE

	for (char c : key)
		data_array.push_back(c);									//KEY

	for (char c : value)
		data_array.push_back(c);									//VALUE

	uint crc = crc32(data_array.data(), data_array.size());
	return crc;
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
	int n = bad_data.size();
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
		string new_name = "wal_logs/wal_" + inttos3(broj_int) + ".log";

		//cout << new_name << endl;

		ret = make_pair(0, new_name);
	}
	return ret;
}

void debugf(Block b) {
	cout << "key: " << b.key.first << " " << b.key.second << endl;
	cout << "data:";
	for (int i = 0; i < block_size; i++) cout << b[i];
	cout << "kraj\n";
}

Block Wal::find_next_empty_block(Block b) {
	bool error;
	int pok = 0;
	Block b2 = b;
	do {
		//cout << "pokusaj: " << pok << endl;
		pok++;

		b2 = bm.read_block(b2.key, error);

		if (b2[block_size - 32] == padding_character && b2[block_size - 31] == padding_character && b2[block_size - 30] == padding_character) return b2;
		//debugf(b2);
		//cout << "Block nadjen\n";

		b2.key = next_key(b2.key, segment_size);
		//cout << "  " << b2.key.first << endl;
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
		record.push_back((crc >> (i * 8)) & 255);				///CRC

	record.push_back(flag);										///FLAG

	for (int i = 7; i >= 0; i--)
		record.push_back((timestamp >> (i * 8)) & 255);			//TIMESTAMP

	record.push_back(tombstone);								//TOMBSTONE

	for (int i = 7; i >= 0; i--)
		record.push_back((key_size >> (i * 8)) & 255);			//KEYSIZE

	for (int i = 7; i >= 0; i--)
		record.push_back((value_size >> (i * 8)) & 255);			//VALUESIZE

	//cout << " extractng key: " << key << endl;
	for (byte c : key)
		record.push_back(c);									//KEY

	//cout << " extractng value: " << value << endl;
	for (byte c : value)
		record.push_back(c);									//VALUE

	//fill_in_padding(record);
	//cout << " size == " << record.size() << endl;
}

void Wal::write_record(string key, string value, byte tombstone) {
	Block first_block(make_pair(0, first_segment));
	Block b;

	b = find_next_empty_block(first_block);
	//debugf(b);
	//cout << endl;
	byte flag = 0;
	ull timestamp = get_timestamp();
	ull key_size = key.size();
	ull value_size = value.size();

	uint crc = calc_crc(flag, timestamp, tombstone, key_size, value_size, key, value);

	vector<byte> record;

	int id = find_empty_pos(b);
	//cout << "id pre : " << id << endl;
	//no space for header at least
	if (block_size - id <= 30) {
		//cout << "Nema mesta za header\n";
		b.key = next_key(b.key, segment_size);
		memset(b.data, padding_character, block_size);
		id = 0;
	}

	/*cout << "id == " << id << endl;
	cout << b.key.first << " " << b.key.second << endl;
	cout << "\n\n\n\n";*/
	int left_space;
	string key2, value2;

	if (key.size() + value.size() + 30 + id <= block_size) {
		//cout << key_size << " " << value_size << endl;
		extract_data(record, crc, flag, timestamp, tombstone, key_size, value_size, key, value);
		//for (char c : record) cout << c << endl;

		memcpy(&b.data[id], &record[0], record.size());

		/*cout << "Upisujem sam rekord:\n";
		cout << "file name: " << b.key.second << endl;
		cout << "id == " << b.key.first << endl;

		cout << " Pre write blocka, data:" << b.data << "kraj\n";
		*/
		bm.write_block(b);
	}
	else {
		flag = 'F';
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
					flag = 'L';
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

			/*cout << flag << endl;
			cout << record.size() << endl;
			cout << key_size << endl;
			cout << value_size << endl;
			cout << key2 << endl;
			cout << value2 << endl;

			cout << "  kopiram na " << id << endl;
			cout << b.key.first << " " << b.key.second << endl;
			cout << b.data << "kraj\n";
			*/
			memcpy(&b.data[id], &record[0], record.size());
			//cout << b.data << "kraj\n";

			//cout << "Upisujem: " << b.key.first << " " << b.key.second << endl;

			bm.write_block(b);
			//return;
			//cout << endl;
			b.key = next_key(b.key, segment_size);
			memset(b.data, padding_character, block_size);
			flag = 'M';
			id = 0;
		}
	}
}

void debug_record(Record r) {
	cout << "CRC: " << r.crc << endl;
	cout << "FLAG: " << r.flag << endl;
	cout << "TIMESTAMP: " << r.timestamp << endl;
	cout << "TOMBSTONE: " << r.tombstone << endl;
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
	write_record(key, "", 1);
}

vector<Record> Wal::get_all_records() {
	vector<Record> ret;
	Record r;
	map<string, int> map_deleted;
	map<string, bool> already_in;

	bool er;
	int rec_pos, valid;
	composite_key tren_key(0, first_segment);
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
			if (r.flag == 'F') {
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

					if (r.flag == 'L') break;
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



	cout << "KRAJ\n";
	for (int i = 0; i < ret.size(); i++) {
		if (map_deleted[ret[i].key] % 2 == 0) {
			//cout << "Erasing\n";
		}
		else {
			if (already_in.find(ret[i].key) != already_in.end()) continue;
			ret2.push_back(ret[i]);
			debug_record(ret[i]);
			already_in[ret[i].key] = 1;
			cout << endl;
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