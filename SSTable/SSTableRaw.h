#pragma once
#include <string>
#include <vector>
#include <fstream>
#include <cstdint>
#include <algorithm>
#include <stdexcept>
#include <iostream>
//#include <additional/sha.h> // OpenSSL za SHA256 heširanje

#include "../Wal/wal.h"
#include "../BloomFilter/BloomFilter.h"
#include "SSTable.h"

/**
 * Struktura za indeks: (key, offset),
 * offset je pozicija u data fajlu odakle pocinje taj zapis
 */
struct IndexEntry {
    std::string key;
    ull offset;
};

struct Summary{
    std::vector<IndexEntry> summary;
    std::string min;
    std::string max;
    uint64_t count; 
};

class SSTableRaw : public SSTable {
public:
    /**
     * Konstruktor prima putanje do tri fajla:
     *  - dataFile (npr. "data.sst")
     *  - indexFile (npr. "index.sst")
     *  - filterFile (npr. "filter.sst")
     */
    SSTableRaw(
        const std::string& dataFile,
        const std::string& indexFile,
        const std::string& filterFile,
        const std::string& summaryFile,
        const std::string& metaFile,
        Block_manager* bmp);

    /**
     * build(...) - gradi SSTable iz niza Record-ova (npr. dobijenih iz memtable).
     * Postupak:
     *   1) Sortira zapise po key
     *   2) Izbacuje one sa tombstone=1 (obrisani)
     *   3) Upisuje binarno u data.sst (dataFile_)
     *   4) Kreira BloomFilter od svih kljuceva
     *   5) Kreira sparse index (key -> offset) i upisuje u indexFile_
     *   6) Snima BloomFilter u filterFile_
     */
    void build(std::vector<Record>& records);

    /**
     * get(key) - dohvatanje vrednosti iz data.sst
     *   - proverava BloomFilter da li kljuc "mozda postoji"
     *   - ako kaze "nema", vraca ""
     *   - ako kaze "mozda ima", binarno pretrazi index, pa cita data fajl
     *     dok ne nadje key ili ga ne predje (data fajl je sortiran)
     */
    std::vector<Record> get(const std::string& key);

    /**
     * (Opciono) range_scan(startKey, endKey):
     *    vraca sve (key,value) koji su izmedju startKey i endKey
     
    std::vector<std::pair<std::string, std::string>>
        range_scan(const std::string& startKey, const std::string& endKey);
     */

protected:
    std::vector<IndexEntry> writeDataMetaFiles(std::vector<Record>& sortedRecords) override;

    // Snima 'index_' u indexFile_
    std::vector<IndexEntry> writeIndexToFile() override;

    // void readIndexFromFile();

    // Snima 'bloom_' u filterFile_
    void writeBloomToFile() const override;

    // Ucitava 'bloom_' iz filterFile_ ako vec nije
    void readBloomFromFile() override;
    void readSummaryHeader() override;

    void writeSummaryToFile() override;
    void writeMetaToFile() const override;
    void readMetaFromFile() override;

    uint64_t findDataOffset(const std::string& key, bool& found) const override;
};