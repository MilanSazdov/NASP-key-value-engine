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
#include "../Utils/VarEncoding.h"


struct IndexEntry {
    std::string key;
    ull offset;
};

struct Summary {
    std::vector<IndexEntry> summary;
    std::string min;
    std::string max;
    uint64_t count;
};

struct TOC
{
    // uint64_t saved_block_size;
	// uint64_t saved_idx_sparsity;
    // uint64_t saved_summ_sparsity; Valjda ne
	uint8_t flags; // Bit 0: Najmanji bit kompresija, sledeci single_file_mode
	uint64_t version = 1; // za upuduce ako se updejtuje TOC
	uint64_t data_offset, data_end;
	uint64_t index_offset;
	uint64_t summary_offset;
	uint64_t filter_offset;
	uint64_t meta_offset;
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
        Block_manager* bmp) :

        dataFile_(dataFile),
        indexFile_(indexFile),
        filterFile_(filterFile),
        summaryFile_(summaryFile),
        metaFile_(metaFile),
        bmp(bmp),
        is_single_file_mode_(false),
        block_size(Config::block_size),
        index_sparsity(Config::index_sparsity),
        summary_sparsity(Config::summary_sparsity),
        toc()
    {
    };
    
    SSTable(const std::string& dataFile,
        Block_manager* bmp) :

        dataFile_(dataFile),
        indexFile_(dataFile),
        filterFile_(dataFile),
        summaryFile_(dataFile),
        metaFile_(dataFile),
        bmp(bmp),
        is_single_file_mode_(true),
        block_size(Config::block_size),
        index_sparsity(Config::index_sparsity),
        summary_sparsity(Config::summary_sparsity),
        toc()
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
    virtual void build(std::vector<Record>& records);

    /**
     * get(key) - dohvatanje vrednosti iz data.sst
     */
    virtual std::vector<Record> get(const std::string& key) = 0;

    /**
     * get(key, n) - Dohvatanje do n elemenata iz sstabele
     *    Pocinje od `key`, skuplja elemente dok ih nema `n`
     *    ili dok ne dodje do kraja data fajla.
     */
     // virtual std::vector<Record> get(const std::string& key, int n) = 0; 

     /**
      * getNextRecord(&offset, &error) - Čita jedan rekord desno od offset i menja offset.
      *    error ce biti postavljen na true ukoliko dodje do kraja fajla.
      */
    virtual Record getNextRecord(uint64_t& offset, bool& error) = 0;

    //virtual bool validate() = 0;

    /**
     * findRecordOffset(key, bool& in_file) - vraća offset u bajtovima gde se prvi Record sa kljucem nalazi u data fajlu.
     *    - Ukoliko sstabela ne sadrzi kljuc, funkcija vraca offset najblizeg desnog elementa i stavlja in_file = false
     *    - Ukoliko sstabela ne sadrzi kljuc i kljuc je veci od najveceg u tabeli, funckija vraca numeric_limits<uint64_t>::max();
     */
    virtual uint64_t findRecordOffset(const std::string& key, bool& in_file) = 0;

    /**
     * getDataStartOffset() - vraća offset u bajtovima gde počinje data segment u data fajlu.
     */
    virtual uint64_t getDataStartOffset() { return toc.data_offset; }

    // i ovo sluzi samo za testiranje
    virtual void printFileNames();

    /**
     * findRecordOffset(key, bool& in_file) - vraća offset u bajtovima gde se prvi Record sa kljucem nalazi u data fajlu.
     *    - Ukoliko sstabela ne sadrzi kljuc, funkcija vraca offset najblizeg desnog elementa i stavlja in_file = false
     *    - Ukoliko sstabela ne sadrzi kljuc i kljuc je veci od najveceg u tabeli, funckija vraca numeric_limits<uint64_t>::max();
     */
    virtual uint64_t findRecordOffset(const std::string& key, bool& in_file) = 0;

    /**
     * getDataStartOffset() - vraća offset u bajtovima gde počinje data segment u data fajlu.
     */
    virtual uint64_t getDataStartOffset() { return toc.data_offset; }

protected:
    // putanje do fajlova
    int block_size;
    std::string dataFile_;
    std::string indexFile_;
    std::string filterFile_;
    std::string summaryFile_;
    std::string metaFile_;

    bool is_single_file_mode_;

    std::vector<IndexEntry> index_;
    Summary summary_;

    BloomFilter bloom_;

    Block_manager* bmp;

    std::string rootHash_; // Cuvamo samo korenski hash
    std::vector<std::string> originalLeafHashes_; // Cuvamo originalne listove Merkle stabla

    size_t summary_sparsity;
    size_t index_sparsity;

    TOC toc;

    // ----- pomoćne metode -----

    virtual void prepare(); // Cita TOC i header summary fajla kako bi se pripremio za citanje

    // Upisuje dataFile_
    // Vraca vector<IndexEntry> da bismo iz njega generisali sparse index
    virtual std::vector<IndexEntry> writeDataMetaFiles(std::vector<Record>& sortedRecords) = 0;

    // Snima 'index_' u indexFile_
    virtual std::vector<IndexEntry> writeIndexToFile() = 0;

    // Snima 'bloom_' u filterFile_
    virtual void writeBloomToFile() = 0;

    // Ucitava 'bloom_' iz filterFile_ ako vec nije
    virtual void readBloomFromFile() = 0;
    virtual void readSummaryHeader() = 0;

    virtual void writeSummaryToFile() = 0;
    virtual void writeMetaToFile() = 0;
    virtual void readMetaFromFile() = 0;



    bool readBytes(void* dst, size_t n, uint64_t& offset, string fileName) const;

    // ovo mora ovde
    template<typename UInt>
    bool readNumValue(UInt& dst, uint64_t& fileOffset, string fileName) const {
        char chunk;
        bool finished = false;
        size_t val_offset = 0;

        dst = 0;

        do {
            if (!readBytes(&chunk, 1, fileOffset, fileName)) {
                return false;
            }
            finished = varenc::template decodeVarint<UInt>(chunk, dst, val_offset);
        } while (finished != true);

        return true;
    }
};