#pragma once
#include <string>
#include "block-manager.h"
#include "wal_types.h"
#include "Config.h"

/*
	koristi se little endian! (znaci 260 = 0x0104, ali po bajtovima je 04 01)!!!
	ofstream outFile("ime_fajla.log", std::ios::binary);
	outFile.write(reinterpret_cast<char*>(&objekat), sizeof(klasa_objekta));
	ovo reinterpret_cast<char*> je tu da bi se objekat serijalizovao u binarni fajl, jer je objekat u memoriji u obliku klase, a ne kao binarni fajl, u sustini to je cast na niz charova 1/0
	bitan je padding, sizeof(objekta) ce uvek biti 4*nesto, znaci moze biti veci, recimo 13 bytova ce biti 16...
*/

/*
	wal je struktura koja podrzava operacije put, get i del u folder logs.
	segment_size je velicina jednog fajla, odnosno broj recorda koje staju u njega
	komanda za ispis binarnog fajla (power shell) format-hex wal_001.log > binary_log.txt

	wal.put("key", "data") upisuje data serijalizovan u fajl
	wal.get("key") cita fajlove logova i vraca da li postoji key
	wal.del("key") upisuje brisanje u fajl

	jedan record sastoji se iz:

	4 bytova crc32 hash sum
	1 byte flag
	8 bytova timestamp (vreme u sekundama kad je upisan record)
	1 byte tombstone (0 ako nije obrisan, 1 ako jeste)
	8 byta keysize
	8 byta datasize
	keysize bytova key
	datasize bytova data

*/

class Wal {
private:

	// this is where writing happens, "points" always to the last empty key (block)
	static composite_key current_block;
	
	// block manager for IO operations on disc
	Block_manager bm;

	// private attribute for segment size
	int segment_size;

	// first segment to read when "get_all_records"
	string min_segment;
	void next_block(composite_key& key);

	composite_key find_next_empty_key(composite_key key);
	void write_record(string key, string value, byte tombstone = (byte)0);

	void update_current_block();
public:
	Wal();

	void put(string key, string data);
	void del(string key);

	// returns ALL records that are in wal structure
	vector<Record> get_all_records();

	// Low Water Mark (valjda) function to delete all wal segments (files) before target_file
	void delete_old_logs(string target_file);
	// void debug_records();
};