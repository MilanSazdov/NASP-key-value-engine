#include <string>
#include <fstream>
#include <iostream>
#include <chrono>
#include <vector>

#include "wal.h"
using namespace std;
typedef long long ll;
typedef unsigned long long ull;
typedef unsigned char byte;
typedef unsigned int uint;


/*
	8 bytova crc32 hash sum
	8 bytova timestamp (vreme u sekundama kad je upisan record)
	1 byte tombstone (0 ako nije obrisan, 1 ako jeste)
	8 byta keysize
	8 byta datasize
	keysize bytova key
	datasize bytova data
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



Wal::Wal() : segment_size(4096) {
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

void Wal::put(string key, string data) {
	cout << "Putting (" << key << ", " << data << ") into wal\n";
	
	uint crc = 0;
	ull timestamp = get_timestamp();
	byte tombstone = 255;
	ull key_size = key.size();
	ull data_size = data.size();
	
	vector<byte> data_array;

	//posto uint ima 4 byta delimo to na 4. Takodje timestamp ali na 8.
	for (int i = 7; i >= 0; i--) 
		data_array.push_back((timestamp >> (i * 8)) & 255);
	
	data_array.push_back(tombstone);

	for (int i = 7; i >= 0; i--) 
		data_array.push_back((key_size >> (i * 8)) & 255);

	for (int i = 7; i >= 0; i--) 
		data_array.push_back((data_size >> (i * 8)) & 255);
	

	crc = crc32(reinterpret_cast<byte*>(&data_array), sizeof(data_array));

	vector<byte> record;
	for (int i = 3; i >= 0; i--)
		record.push_back((crc >> (i * 8)) & 255);
	for (byte c : data_array)
		record.push_back(c);
	
	for (byte c : key) 
		record.push_back(c);
	
	//cout << endl;
	for (byte c : data)
		record.push_back(c);

	ofstream outFile("logs/wal_001.log", std::ios::binary | std::ios::app);
	//cout << record.size() << endl;

	outFile.write(reinterpret_cast<const char*>(record.data()), record.size());
	outFile.close();
}
string Wal::get(string key) {
	// read from file
	return "data";
}
void Wal::del(string key) {
	// delete from file
}
