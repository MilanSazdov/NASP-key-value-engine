#include <regex>

#include "LSMManager.h"

namespace fs = std::filesystem;

LSMManager::LSMManager(const std::string& base_directory,
	std::string strategy,
	int max_sstable_levels,
	int max_levels,
	int multiplier_level,
	SSTManager& sstm)
	: base_directory_(base_directory),
	compaction_strategy_(strategy),
	max_levels_(max_levels),
	max_sstable_levels_(max_sstable_levels),
	level_size_multiplier_(multiplier_level),
	sstManager_(sstm)
{
	std::cout << "[LSMManager] Kreiran sa " << compaction_strategy_ << " strategijom." << std::endl;
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

				CompactionPlan plan;
				plan.inputs_level_1 = tables_to_merge;
				plan.output_level = i + 1;

				do_compaction(plan);
				compaction_happened_in_this_cycle = true;
				break;

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

void LSMManager::do_compaction(const CompactionPlan& plan) {
	if (plan.inputs_level_1.empty()) return;

	std::cout << "  -> Započinjem spajanje " << plan.inputs_level_1.size() << " fajlova na nivo " << plan.output_level << std::endl;

	std::vector<std::unique_ptr<SSTableIterator>> iterators;
	for (const auto& meta : plan.inputs_level_1) {
		iterators.push_back(std::make_unique<SSTableIterator>(meta, Config::block_size, &sstManager_.get_block_manager()));
	}

	MergeIterator merge_iterator(std::move(iterators));

	
	std::vector<Record> compacted_records;
	while (merge_iterator.hasNext()) {
		compacted_records.push_back(merge_iterator.next());
	}

	if (!compacted_records.empty()) {
		sstManager_.write(compacted_records, plan.output_level);
	}

	for (const auto& meta : plan.inputs_level_1) {
		// ... logika za brisanje svih fajlova vezanih za meta ...
	}

	std::cout << "  -> Spajanje uspesno zavrseno." << std::endl;
}

std::vector<SSTableMetadata> LSMManager::findSSTablesOnLevel(int level) const {
	std::vector<SSTableMetadata> found_tables;
	std::string level_path = getLevelPath(level);

	if (!fs::exists(level_path) || !fs::is_directory(level_path)) {
		return found_tables;
	}

	const std::regex file_pattern("([a-z]+)_(raw|comp)_(multi|single)_(\\d+)\\.(db|dat)");

	for (const auto& entry : fs::directory_iterator(level_path)) {
		if (!entry.is_regular_file()) continue;

		std::string filename = entry.path().filename().string();
		std::smatch matches;

		if (std::regex_match(filename, matches, file_pattern) &&
			(matches[1].str() == "data" || matches[1].str() == "sstable")) {

			try {
				SSTableMetadata meta;
				meta.level = level;

				std::string type_prefix = matches[1].str();
				std::string compression_str = matches[2].str();
				std::string format_str = matches[3].str();
				std::string id_str = matches[4].str();

				meta.file_id = std::stoi(id_str);
				bool is_comp = (compression_str == "comp");
				bool is_single = (format_str == "single");

				std::unique_ptr<SSTable> sst_reader;

				if (is_single) {
					std::string path = entry.path().string();
					meta.data_path = meta.index_path = meta.summary_path = meta.filter_path = meta.meta_path = path;
				}
				else {
					std::string base_name = compression_str + "_" + format_str + "_" + id_str;
					meta.data_path = level_path + "/data_" + base_name + ".db";
					meta.index_path = level_path + "/index_" + base_name + ".db";
					meta.summary_path = level_path + "/summary_" + base_name + ".db";
					meta.filter_path = level_path + "/filter_" + base_name + ".db";
					meta.meta_path = level_path + "/meta_" + base_name + ".db";

					if (is_comp) {
						sst_reader = std::make_unique<SSTableComp>(
							meta.data_path, meta.index_path, meta.filter_path, meta.summary_path, meta.meta_path,
							&sstManager_.get_block_manager(),
							sstManager_.get_key_to_id_map(),
							sstManager_.get_id_to_key_map(),
							sstManager_.get_next_id()
						);
					}
					else {
						sst_reader = std::make_unique<SSTableRaw>(
							meta.data_path, meta.index_path, meta.filter_path, meta.summary_path, meta.meta_path,
							&sstManager_.get_block_manager()
						);
					}
				}

				sst_reader->readSummaryHeader();

				meta.min_key = sst_reader->getMinKey();
				meta.max_key = sst_reader->getMaxKey();
				meta.file_size = sst_reader->getTotalSize();

				found_tables.push_back(meta);

			}
			catch (const std::exception& e) {
				std::cerr << "Greska pri parsiranju ili citanju metapodataka za fajl "
					<< filename << ": " << e.what() << std::endl;
			}
		}
	}

	return found_tables;
}