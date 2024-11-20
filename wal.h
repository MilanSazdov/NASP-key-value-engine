#pragma once
#include <string>
using namespace std;

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
	komanda za ispis binarnog fajla format-hex wal_001.log > binary_log.txt

	wal.put("key", "data") upisuje data serijalizovan u fajl
	wal.get("key") cita fajlove logova i vraca da li postoji key
	wal.del("key") upisuje brisanje u fajl

	jedan record sastoji se iz:

	8 bytova crc32 hash sum
	8 bytova timestamp (vreme u sekundama kad je upisan record)
	1 byte tombstone (0 ako nije obrisan, 1 ako jeste)
	8 byta keysize
	8 byta datasize
	keysize bytova key
	datasize bytova data

*/
class Wal {
	int segment_size;
public:
	Wal();
	Wal(int segment_size);
	void put(string key, string data);
	string get(string key);
	void del(string key);
};