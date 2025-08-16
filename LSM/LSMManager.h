#pragma once

#include <string>
#include <vector>
#include <filesystem>
#include <memory>


#include "SSTManager.h"
#include "wal.h"
#include "SSTable.h"
#include "SSTableComp.h"
#include "SSTableRaw.h"

// Metapodaci o jednom SSTable fajlu
struct SSTableMetadata
{
	int level;
	int file_id;
	uint64_t file_size;
	std::string min_key;
	std::string max_key;

	// Putanje do svih komponenti SSTable
	std::string data_path;
	std::string index_path;
	std::string summary_path;
	std::string filter_path;
	std::string meta_path;

	// Potrebno za std::remove_if
	bool operator==(const SSTableMetadata& other) const {
		return file_id == other.file_id && level == other.level;
	}
};


class LSMManager {
public:
    
    LSMManager(const std::string& base_directory, std::string strategy, int max_levels, int max_sstable_levels,int multiplier_level, SSTManager& sstm);

    // glavna ulazna tacka koju poziva MemtableManager
	void checkIfCompactionIsNeeded();

private:
    
    std::string base_directory_;
    SSTManager& sstManager_;
	int max_levels_;
    int max_sstable_levels_;
    int level_size_multiplier_;
	std::string compaction_strategy_;

	std::vector<int> current_sstable_on_level_;
	std::vector<int> max_sstable_on_level_;

    // pomocna metoda koja skenira disk za jedan nivo i vraca metapodatke
	std::vector<SSTableMetadata> findSSTablesOnLevel(int level) const;
    
	bool runSizeTieredCompaction();
	bool runLeveledCompaction();


	void loadCurrentLevels();
    int countSSTablesOnLevel(int level) const;

    void merge_files(const std::vector<SSTableMetadata>& files_to_merge, int output_level);
    std::string getLevelPath(int level) const;
    
    // Centralna struktura koja cuva stanje LSM stabla u memoriji
    std::vector<std::vector<SSTableMetadata>> levels_;

};

