#pragma once

#include <fstream>
#include <string>
#include <iostream>
#include <stdexcept>
#include <algorithm>

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

	// Memtable podesavanja
	static std::string memtable_type;
	static size_t memtable_instances;
	static size_t memtable_max_size;

	// LSM & Kompakcije podesavanja
	static std::string compaction_strategy;
	static int max_levels;

	// Leveled Compaction
	static int l0_compaction_trigger;
	static uint64_t target_file_size_base;
	static int level_size_multiplier;

	// Size-Tiered Compaction
	static int min_threshold;
	static int max_threshold;
	
	// Data and Wal directory
	static std::string data_directory;
	static std::string wal_directory;

	static void load_init_configuration();
};
