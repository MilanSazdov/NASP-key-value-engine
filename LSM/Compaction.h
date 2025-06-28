#pragma once

#include <string>
#include <vector>
#include <optional>
#include <cstdint> // za uint64_t
#include "wal.h"

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

// Opisuje jednu operaciju kompakcije: koji fajlovi se spajaju i gde ide izlaz
struct CompactionPlan {
	std::vector<SSTableMetadata> inputs_level_1;
	std::vector<SSTableMetadata> inputs_level_2; // Koristi se samo u LCS
	int output_level;
};


// Apstraktna bazna klasa (interfejs) za sve strategije kompakcije
class CompactionStrategy {
public:
	virtual ~CompactionStrategy() = default;

	// Glavna metoda koja odlucuje da li je kompakcija potrebna i koja
	// Prima trenutno stanje nivoa i vraca plan ako je kompakcija potrebna
	virtual std::optional<CompactionPlan> pickCompaction(
		const std::vector<std::vector<SSTableMetadata>>& levels
	) const = 0;
};

