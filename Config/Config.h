#pragma once

#include <fstream>
#include <string>
#include <iostream>
#include <stdexcept>
#include <algorithm>
#include <cstdint>

class Config {
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

	// Data folder for: sstable/lsm , memtable, and Wal directory
	static std::string data_directory;
	static std::string wal_directory;

	// SSTable
	static int index_sparsity;
	static bool compress_sstable;

	static void load_init_configuration();
};