#pragma once
#include "SSTable.h"
#include <vector>

class SSTableSingleFile : public SSTable {
private:

    struct TOC {
        uint64_t magic_number = 0xABCDDCBA12344321;
        uint64_t version = 1;
        uint64_t flags = 0; // Bit 0: is_compressed
        uint64_t saved_block_size = 0;
        uint64_t saved_sparsity = 0;

        uint64_t data_offset, data_len;
        uint64_t index_offset, index_len;
        uint64_t summary_offset, summary_len;
        uint64_t filter_offset, filter_len;
        uint64_t meta_offset, meta_len;
    };

    TOC toc_; // cuvamo ucitan TOC
    bool toc_loaded_ = false;

    void readTOC();
    std::vector<byte> readComponent(uint64_t component_offset, uint64_t component_len);

public:
    SSTableSingleFile(const std::string& single_file_path, Block_manager* bmp, bool is_compressed);

    void build(std::vector<Record>& records) override;

    void readBloomFromFile() override;
    void readSummaryHeader() override;
    void readMetaFromFile() override;
    
    uint64_t findDataOffset(const std::string& key, bool& found) const override;
    std::vector<Record> get(const std::string& key) override;
    bool validate() override;

protected:
    // pomocne metode za build => pisu u memorijske bafere
    std::vector<IndexEntry> writeDataToBuffer(std::vector<Record>& sortedRecords, std::ostream& out);
    std::vector<IndexEntry> writeIndexToBuffer(std::ostream& out);
    void writeSummaryToBuffer(std::ostream& out);
    void writeBloomToBuffer(std::ostream& out) const;
    void writeMetaToBuffer(std::ostream& out) const;
};