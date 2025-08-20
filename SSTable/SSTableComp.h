#pragma once
#include <string>
#include <vector>
#include <fstream>
#include <cstdint>
#include <algorithm>
#include <stdexcept>
#include <iostream>
#include <map>
//#include <additional/sha.h> // OpenSSL za SHA256 heširanje

#include "../Wal/wal.h"
#include "../BloomFilter/BloomFilter.h"
#include "SSTable.h"


class SSTableComp : public SSTable {
public:
    /**
     * Konstruktor prima putanje do tri fajla:
     *  - dataFile (npr. "data.sst")
     *  - indexFile (npr. "index.sst")
     *  - filterFile (npr. "filter.sst")
     */
    SSTableComp(
        const std::string& dataFile,
        const std::string& indexFile,
        const std::string& filterFile,
        const std::string& summaryFile,
        const std::string& metaFile,
        Block_manager* bmp,
        unordered_map<string, uint32_t>& key_to_id,
        vector<string>& id_to_key,
        uint32_t& nextId);


    SSTableComp(const std::string& dataFile,
        Block_manager* bmp,
        unordered_map<string, uint32_t>& map,
        vector<string>& id_to_key,
        uint32_t& nextId);

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
     // void build(std::vector<Record>& records) override;

     /**
      * get(key) - dohvatanje vrednosti iz data.sst
      *   - proverava BloomFilter da li kljuc "mozda postoji"
      *   - ako kaze "nema", vraca ""
      *   - ako kaze "mozda ima", binarno pretrazi index, pa cita data fajl
      *     dok ne nadje key ili ga ne predje (data fajl je sortiran)
      */
    std::vector<Record> get(const std::string& key) override;

    // Funkcija vraca do `n` rekorda, pocinje pretragu od `key`. Vratice manje od `n` ako dodje do kraja SSTabele.
    // std::vector<Record> get(const std::string& key, int n) override;

    Record getNextRecord(uint64_t& offset, bool& error) override;

    //bool validate() override;

    uint64_t findRecordOffset(const std::string& key, bool& in_file) override;

protected:
    std::vector<IndexEntry> writeDataMetaFiles(std::vector<Record>& sortedRecords) override;

    // Snima 'index_' u indexFile_
    std::vector<IndexEntry> writeIndexToFile() override;

    // void readIndexFromFile();

    // Snima 'bloom_' u filterFile_
    void writeBloomToFile() override;

    // Ucitava 'bloom_' iz filterFile_ ako vec nije
    void readBloomFromFile() override;
    void readSummaryHeader() override;

    void writeSummaryToFile() override;
    void writeMetaToFile() override;
    void readMetaFromFile() override;


    uint64_t summary_data_start;

    unordered_map<string, uint32_t>& key_to_id;
    vector<string>& id_to_key;
    uint32_t& nextID;

};