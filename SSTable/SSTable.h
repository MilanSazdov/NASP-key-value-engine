#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <algorithm>
#include <stdexcept>
#include <iostream>

#include "../Wal/wal.h"
#include "../BloomFilter/BloomFilter.h"
#include "../MerkleTree/MerkleTree.h"

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

class SSTable {
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
        const std::string& metaFile,
        Block_manager* bmp): 
        dataFile_(dataFile),
        indexFile_(indexFile),
        filterFile_(filterFile),
        summaryFile_(summaryFile),
        metaFile_(metaFile), 
        bmp(bmp),
        block_size(Config::block_size),
        SPARSITY(Config::index_sparsity)
        {
        };
        
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
    virtual void build(std::vector<Record>& records) = 0;

    /**
     * get(key) - dohvatanje vrednosti iz data.sst
     *   - proverava BloomFilter da li kljuc "mozda postoji"
     *   - ako kaze "nema", vraca ""
     *   - ako kaze "mozda ima", binarno pretrazi index, pa cita data fajl
     *     dok ne nadje key ili ga ne predje (data fajl je sortiran)
     */
    virtual std::vector<Record> get(const std::string& key) = 0;

    /**
     * (Opciono) range_scan(startKey, endKey):
     *    vraca sve (key,value) koji su izmedju startKey i endKey
     
    std::vector<std::pair<std::string, std::string>>
        range_scan(const std::string& startKey, const std::string& endKey);
    */

	virtual bool validate() = 0;

protected:
    // putanje do fajlova
    int block_size;
    std::string dataFile_;
    std::string indexFile_;
    std::string filterFile_;
    std::string summaryFile_;
    std::string metaFile_;

    std::vector<IndexEntry> index_;
    Summary summary_;

    BloomFilter bloom_;

    Block_manager* bmp;

    std::string rootHash_; // Cuvamo samo korenski hash
	std::vector<std::string> originalLeafHashes_; // Cuvamo originalne listove Merkle stabla

    size_t SPARSITY;

    // ----- pomoćne metode -----

    // Upisuje dataFile_
    // Vraca vector<IndexEntry> da bismo iz njega generisali sparse index
    virtual std::vector<IndexEntry> writeDataMetaFiles(std::vector<Record>& sortedRecords) = 0;

    // Snima 'index_' u indexFile_
    virtual std::vector<IndexEntry> writeIndexToFile() = 0;

    // Snima 'bloom_' u filterFile_
    virtual void writeBloomToFile() const = 0;

    // Ucitava 'bloom_' iz filterFile_ ako vec nije
    virtual void readBloomFromFile() = 0;
    virtual void readSummaryHeader() = 0;

    virtual void writeSummaryToFile() = 0;
    virtual void writeMetaToFile() const = 0;
    virtual void readMetaFromFile() = 0;

    virtual uint64_t findDataOffset(const std::string& key, bool& found) const = 0;


    bool readBytes(void *dst, size_t n, uint64_t& offset, string fileName) const;



};