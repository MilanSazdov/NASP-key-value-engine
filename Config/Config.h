#pragma once

class Config {
	// kontam da ovde mozemo da spicimo jos neke stvari. Ne znam da li ovde implementacija memtable (pa se bira, BStablo, SkipLista, sta vec)
	// da li da stavimo neke putanje npr wal folder, data folder, itd... pa da i to bude konfiguraciono
	
	// cini mi se treba neke stvari za SSTAble, ali posto nisam radio ne znam tacno sta, to ubacite @Andrej, @Milan
private:
	Config() = default;		// Prevent instantiation

public:
	static void debug();
	static int cache_capacity;	//CACHE:		 in blocks
	static int block_size;	    //BLOCK MANAGER: in bytes
	static int segment_size;	//WAL:			 in records

	static void load_init_configuration();
};

