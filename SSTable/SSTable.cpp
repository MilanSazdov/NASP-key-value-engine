#include "SSTable.h"

void SSTable::build(std::vector<Record>& records)
{
    if (records.empty()) {
        std::cerr << "[SSTable] build: Nema zapisa.\n";
        return;
    }

	std::sort(records.begin(), records.end(),
		[](const Record& a, const Record& b) { return a.key < b.key; });

    if (is_single_file_mode_) {
        std::cout << "[SSTable] Započinjem build u SINGLE-FILE modu..." << std::endl;

        std::stringstream data_stream, index_stream, summary_stream, filter_stream, meta_stream;

		index_ = writeDataToBuffer(records, data_stream);
        std::vector<IndexEntry> full_index = writeIndexToBuffer(index_stream);

        bloom_ = BloomFilter(records.size(), 0.01);
        for (const auto& r : records) bloom_.add(r.key);
        writeBloomToBuffer(filter_stream);

        buildSummary(full_index);
        writeSummaryToBuffer(summary_stream);

        std::vector<std::string> data_for_merkle;
        for (const auto& r : records) data_for_merkle.push_back(r.key + r.value);
        MerkleTree mt(data_for_merkle);
        rootHash_ = mt.getRootHash();
        originalLeafHashes_ = mt.getLeaves();
        writeMetaToBuffer(meta_stream);

        // Ovde treba sklapanje u jedan veliki payload i upisivanje na disk
        // Dolazi logika za TOC i pisanje preko Block Managera
    }
    else {
        std::cout << "[SSTable] Započinjem build u MULTI-FILE modu..." << std::endl;


        index_ = writeDataMetaFiles(records);

        BloomFilter bf(records.size(), 0.01);
        for (const auto& r : records) {
            bf.add(r.key);
        }
        bloom_ = bf;

        // Index u fajl
        std::vector<IndexEntry> summaryAll = writeIndexToFile();

        summary_.summary.reserve(summaryAll.size() / SPARSITY + 1);

        for (size_t i = 0; i < summaryAll.size(); i += SPARSITY) {
            summary_.summary.push_back(summaryAll[i]);
        }
        if (!summaryAll.empty() &&
            ((summaryAll.size() - 1) % SPARSITY) != 0) {
            summary_.summary.push_back(summaryAll.back());
        }

        if (!summaryAll.empty()) {
            auto minEntry = std::min_element(
                summaryAll.begin(), summaryAll.end(),
                [](const IndexEntry& a, const IndexEntry& b) { return a.key < b.key; });
            auto maxEntry = std::max_element(
                summaryAll.begin(), summaryAll.end(),
                [](const IndexEntry& a, const IndexEntry& b) { return a.key < b.key; });

            summary_.min = minEntry->key;
            summary_.max = maxEntry->key;
        }

        // Bloom, meta, summary u fajl
        writeBloomToFile();
        writeSummaryToFile();
        writeMetaToFile();
    }

    std::cout << "[SSTable] build: upisano " << records.size()
        << " zapisa u " << dataFile_ << ".\n";
}

bool SSTable::readBytes(void *dst, size_t n, uint64_t& offset, string fileName) const
{
    if(n==0) return true;

    bool error = false;

    int block_id = offset / block_size;
    uint64_t block_pos = offset % block_size;  

    vector<byte> block = bmp->read_block({block_id++, fileName}, error);
    if(error) return !error;

    char* out = reinterpret_cast<char*>(dst);

    while (n > 0) {
        if (block_pos == block_size) {
            // fetch sledeci
            block = bmp->read_block({block_id++, fileName}, error);
            if(error) return !error;

            block_pos = 0;
        }
        size_t take = min(n, block_size - block_pos);
        std::memcpy(out, block.data() + block_pos, take);
        out += take;
        offset += take;
        block_pos += take;
        n -= take;
    }

    return !error;
}