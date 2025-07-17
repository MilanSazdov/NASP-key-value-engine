#pragma once

#include <string>
#include <vector>
#include <filesystem>
#include <memory>


#include "SSTManager.h"
#include "wal.h"
#include "Compaction.h"
#include "SSTable.h"
#include "SSTableComp.h"
#include "SSTableRaw.h"
#include "SSTableIterator.h"
#include "MergeIterator.h"


class LSMManager {
public:
    
    LSMManager(const std::string& base_directory, std::string strategy, int max_levels, int max_sstable_levels,int multiplier_level, SSTManager& sstm);

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

