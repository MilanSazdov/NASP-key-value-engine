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
	static int level_size_multiplier;

	// Size-Tiered Compaction
	static int max_number_of_sstable_on_level;

	// SSTable
	static int index_sparsity;
	static int summary_sparsity;
	static bool compress_sstable;
	static bool sstable_single_file;

	static void load_init_configuration();
};