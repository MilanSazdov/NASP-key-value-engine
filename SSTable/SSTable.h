#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <cstdint>
#include <algorithm>
#include <stdexcept>

// Uvozimo 'Record' iz wal.h
#include "../Wal/wal.h"
// Uvozimo BloomFilter
#include "../BloomFilter/BloomFilter.h"

/**
 * Struktura za indeks: (key, offset),
 * offset je pozicija u data fajlu odakle pocinje taj zapis
 */
struct IndexEntry {
    std::string key;
    uint64_t offset;
};

struct Summary
{
    std::vector<IndexEntry> summary;
    string min;
    string max;
};

class     SSTable {
public:
    /**
     * Konstruktor prima putanje do tri fajla:
     *  - dataFile (npr. "data.sst")
     *  - indexFile (npr. "index.sst")
     *  - filterFile (npr. "filter.sst")
     */
    SSTable(const std::string& dataFile,
        const std::string& indexFile,
        const std::string& filterFile,
        const std::string& summaryFile,
        const std::string& metaFile);

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
    void build(const std::vector<Record>& records);

    /**
     * get(key) - dohvatanje vrednosti iz data.sst
     *   - proverava BloomFilter da li kljuc "mozda postoji"
     *   - ako kaze "nema", vraca ""
     *   - ako kaze "mozda ima", binarno pretrazi index, pa cita data fajl
     *     dok ne nadje key ili ga ne predje (data fajl je sortiran)
     */
    vector<Record> get(const std::string& key);

    /**
     * (Opciono) range_scan(startKey, endKey):
     *    vraca sve (key,value) koji su izmedju startKey i endKey
     */
    std::vector<std::pair<std::string, std::string>>
        range_scan(const std::string& startKey, const std::string& endKey);

private:
    // putanje do tri fajla
    std::string dataFile_;
    std::string indexFile_;
    std::string filterFile_;
    std::string summaryFile_;
    std::string metaFile_;

    // u memoriji cuvamo index i bloom, ako zelimo, inace ucitavamo "on demand"
    std::vector<IndexEntry> index_;
    Summary summary_;
    BloomFilter bloom_;

    std::vector<std::string> tree;
    string rootHash;

    string buildMerkleTree(const vector<string>& leaves);

    // ----- pomoćne metode -----

    // Upisuje binarno (Record by record) u dataFile_
    // Vraca vector<IndexEntry> da bismo iz njega generisali sparse index
    std::vector<IndexEntry> writeDataMetaFiles(const std::vector<Record>& sortedRecords) const;

    // Snima 'index_' u indexFile_ (binarno)
    std::vector<IndexEntry> writeIndexToFile();

    // Ucitava 'index_' iz indexFile_ (binarno) ako je prazan
    void readIndexFromFile();

    // Snima 'bloom_' u filterFile_ (binarno)
    void writeBloomToFile() const;

    // Ucitava 'bloom_' iz filterFile_ ako vec nije
    void readBloomFromFile();
    void readSummaryFromFile();

    void writeSummaryToFile();
    void writeMetaToFile() const;
    void readMetaFromFile();

    // Binarna pretraga po 'index_' da nadjemo offset "bloka" gde treba da pocnemo citanje
    uint64_t findDataOffset(const std::string& key, bool& found) const;
};
