#pragma once

#include <string>
#include <vector>
#include <filesystem>
#include "SSTManager.h"
#include "wal.h"

class LSMManager {

private:

	std::string base_directory_; // gde se cuvaju podaci "./data/"
	int max_level_;
	int compaction_threshold_; // koliko prima sstable pre same kompakcije, za level 0

	SSTManager sstManager_;

public:

	LSMManager(const std::string& base_directory, int max_level, int compaction_threshold);

	void initialize(); // pravi direktorijume level_x

	std::string getLevelPath(int level) const;

	// upisuje novi sstable u level 0
	void flushToLevel0(const std::vector<Record>& records);

	// gleda da li je vreme za kompakciju
	void checkAndTriggerCompaction();

	// Pokrece kompakciju iz level_x u level_x+1
	void compactLevel(int level);

};

