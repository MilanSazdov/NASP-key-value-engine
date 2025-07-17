#include <regex>

#include "LSMManager.h"

namespace fs = std::filesystem;


// ovde razmisliti sta treba sa SSTManagerom
LSMManager::LSMManager(const std::string& base_directory, std::string strategy, int max_sstable_levels, int max_levels, int multiplier_level) {
	this->base_directory_ = base_directory;
	this->max_levels_ = max_levels;
	this->max_sstable_levels_ = max_sstable_levels;
	this->level_size_multiplier_ = multiplier_level;
	this->compaction_strategy_ = strategy;
}

int LSMManager::countSSTablesOnLevel(int level) const {
	int sstable_count = 0;
	std::string level_path = getLevelPath(level);
	if (!fs::exists(level_path)) return 0;

	const std::regex file_pattern("data_.*_(\\d+)\\.db|sstable_.*_(\\d+)\\.dat");

	for (const auto& entry : fs::directory_iterator(level_path)) {
		if (entry.is_regular_file()) {
			if (std::regex_search(entry.path().filename().string(), file_pattern)) {
				sstable_count++;
			}
		}
	}
	return sstable_count;
}

/*
int LSMManager::countSSTablesOnLevel(int level) {
	
	int sstable_count = 0;
	std::string level_path = getLevelPath(level);

	if (!fs::exists(level_path) || !fs::is_directory(level_path)) {
		return 0;
	}
	
	const std::regex file_pattern("([a-z]+)_(raw|comp)_(multi|single)_(\\d+)\\.(db|dat)");
	
	for (const auto& entry : fs::directory_iterator(level_path)) {
		if (!entry.is_regular_file()) {
			continue;
		}

		std::string filename = entry.path().filename().string();
		std::smatch matches;

		if (std::regex_match(filename, matches, file_pattern)) {

			std::string component_type = matches[1].str();
			std::string format_type = matches[3].str();

			if (format_type == "single" && component_type == "sstable") {
				sstable_count++;
			}
			else if (format_type == "multi" && component_type == "data") {
				sstable_count++;
			}
		}
	}

	return sstable_count;

}
*/
void LSMManager::loadCurrentLevels() {

	current_sstable_on_level_.clear();
	current_sstable_on_level_.resize(max_levels_);

	for (int level = 0; level < max_levels_; level++) {
		current_sstable_on_level_[level] = countSSTablesOnLevel(level);
	}

	if (compaction_strategy_ == "size_tiered") {
		std::cout << "  -> Konfigurišem limite za Size-Tiered strategiju." << std::endl;
		for (int level = 0; level < max_levels_ - 1; level++) {
			max_sstable_on_level_[level] = max_sstable_levels_;
		}

		max_sstable_on_level_[max_levels_ - 1] = std::numeric_limits<int>::max();

	}
	else if (compaction_strategy_ == "leveled") {
		
		std::cout << "  -> Konfigurišem limite za Leveled strategiju." << std::endl;

		max_sstable_on_level_[0] = Config::l0_compaction_trigger;

		max_sstable_on_level_[1] = 2;
		for (int level = 2; level < max_levels_ - 1; level++) {
			max_sstable_on_level_[level] = max_sstable_on_level_[level - 1] * level_size_multiplier_;
		}
		
		max_sstable_on_level_[max_levels_ - 1] = std::numeric_limits<int>::max();
	}

	std::cout << "[LSMManager] Stanje nivoa uspešno učitano." << std::endl;
}

void LSMManager::checkIfCompactionIsNeeded() {

	if (compaction_strategy_ == "size_tiered") {
		
		runSizeTieredCompaction();
	}
	else if (compaction_strategy_ == "leveled") {
		runLeveledCompaction();
	}
	else {
		throw std::runtime_error("Unknown compaction strategy: " + compaction_strategy_);
	}
}

bool LSMManager::runSizeTieredCompaction() {
	bool compaction_was_performed_overall = false;

	while (true) {

		bool compaction_happened_in_this_cycle = false;
		for (int i = 0; i < max_levels_ - 1; ++i) {

			int table_count = current_sstable_on_level_[i];

			if (table_count >= max_sstable_levels_) {

				std::cout << "[LSMManager] Size-Tiered uslov ispunjen na nivou " << i
					<< ". Broj tabela: " << table_count << " (prag: " << max_sstable_levels_ << ")" << std::endl;

				auto tables_to_merge = findSSTablesOnLevel(i);

				// TODO: Sada te tabele treba mergovati i prebaciti na novi nivo

			}
		}
		
		if (!compaction_happened_in_this_cycle) {
			break;
		}
	}

	if (compaction_was_performed_overall) {
		std::cout << "[LSMManager] Size-Tiered ciklus kompakcija završen." << std::endl;
	}
	else {
		std::cout << "[LSMManager] Size-Tiered kompakcija nije bila potrebna." << std::endl;
	}

	return compaction_was_performed_overall;
	
}