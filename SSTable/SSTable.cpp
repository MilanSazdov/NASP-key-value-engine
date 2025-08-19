#include "SSTable.h"

void SSTable::build(std::vector<Record>&records)
{
    std::sort(records.begin(), records.end(),
        [](auto const& a, auto const& b) {
            return a.key < b.key;
        });

    if (records.empty()) {
        std::cerr << "[SSTable] build: Nema zapisa.\n";
        return;
    }

    // Data u fajl
    std::vector<IndexEntry> indexAll = writeDataMetaFiles(records);

    index_.reserve(indexAll.size() / index_sparsity + 1);
    for (size_t i = 0; i < indexAll.size(); i += index_sparsity) {
        index_.push_back(indexAll[i]);
    }
    if (!indexAll.empty() &&
        ((indexAll.size() - 1) % summary_sparsity) != 0) {
        index_.push_back(indexAll.back());
    }

    BloomFilter bf(records.size(), 0.01);
    for (const auto& r : records) {
        bf.add(r.key);
    }
    bloom_ = bf;

    // Index u fajl
    std::vector<IndexEntry> summaryAll = writeIndexToFile();

    // Pravimo summary
    summary_.summary.reserve(summaryAll.size() / summary_sparsity + 1);

    for (size_t i = 0; i < summaryAll.size(); i += summary_sparsity) {
        summary_.summary.push_back(summaryAll[i]);
    }
    if (!summaryAll.empty() &&
        ((summaryAll.size() - 1) % summary_sparsity) != 0) {
        summary_.summary.push_back(summaryAll.back());
    }

    if (!summaryAll.empty()) {
        auto minEntry = std::min_element(
            indexAll.begin(), indexAll.end(),
            [](const IndexEntry& a, const IndexEntry& b) { return a.key < b.key; });
        auto maxEntry = std::max_element(
            indexAll.begin(), indexAll.end(),
            [](const IndexEntry& a, const IndexEntry& b) { return a.key < b.key; });

        summary_.min = minEntry->key;
        summary_.max = maxEntry->key;
    }

    // Bloom, meta, summary u fajl
    writeSummaryToFile();
    writeBloomToFile();
    writeMetaToFile();


    // Pisemo toc
    // Moze da se zameni memcpy(&payload[0], ...) ali ce padovati 
    string payload;
    payload.resize(sizeof(toc));
    std::memcpy(&payload[0], &toc, sizeof(toc));
    // payload.append(reinterpret_cast<char*>(&toc.saved_block_size), sizeof(toc.saved_block_size));
    // payload.append(reinterpret_cast<char*>(&toc.saved_idx_sparsity), sizeof(toc.saved_idx_sparsity));
    // payload.append(reinterpret_cast<char*>(&toc.saved_summ_sparsity), sizeof(toc.saved_summ_sparsity));
    // payload.append(reinterpret_cast<char*>(&toc.flags), sizeof(toc.flags));
    // payload.append(reinterpret_cast<char*>(&toc.version), sizeof(toc.version));

    // payload.append(reinterpret_cast<char*>(&toc.data_offset), sizeof(toc.data_offset));
    // payload.append(reinterpret_cast<char*>(&toc.data_end), sizeof(toc.data_end));

    // payload.append(reinterpret_cast<char*>(&toc.index_offset), sizeof(toc.index_offset));
    // payload.append(reinterpret_cast<char*>(&toc.summary_offset), sizeof(toc.summary_offset));
    // payload.append(reinterpret_cast<char*>(&toc.filter_offset), sizeof(toc.filter_offset));
    // payload.append(reinterpret_cast<char*>(&toc.meta_offset), sizeof(toc.meta_offset));


    int block_id = 0;
    uint64_t total_bytes = payload.size();
    uint64_t offset = 0;

    while (offset + block_size <= total_bytes) {
        string chunk = payload.substr(offset, block_size);
        bmp->write_block({ block_id++, dataFile_ }, chunk);
        offset += block_size;
    }

    if (offset < total_bytes) {
        std::string chunk = payload.substr(offset);
        bmp->write_block({ block_id++, dataFile_ }, chunk);
    }


    std::cout << "[SSTable] build: upisano " << records.size()
        << " zapisa u " << dataFile_ << ".\n";
}

void SSTable::prepare() {
    // if(toc.data_offset != 0) return; // Gledamo da li smo vec inicijalizovali.
                                      // TODO: toc.data_offset je setovan u write pathu ali nije u read pathu van ove funkcije.
                                      // To ne bi trebalo da bude problem, ali proveri svakako.

    uint64_t offset = 0;

    if (!readBytes(&toc, sizeof(toc), offset, dataFile_)) {
        return;
    }

    /*
        if (!readBytes(&block_size, sizeof(uint64_t), offset, dataFile_)) {
            return;
        }
        if (!readBytes(&index_sparsity, sizeof(uint64_t), offset, dataFile_)) {
            return;
        }
        if (!readBytes(&summary_sparsity, sizeof(uint64_t), offset, dataFile_)) {
            return;
        }

        offset += sizeof(uint8_t); // Preskacemo flags bajt zato sto vec znamo da li smo kompresovani i single file

        if (!readBytes(&toc.version, sizeof(uint64_t), offset, dataFile_)) {
            return;
        }

        if (!readBytes(&toc.data_offset, sizeof(uint64_t), offset, dataFile_)) {
            return;
        }

        if (!readBytes(&toc.data_end, sizeof(uint64_t), offset, dataFile_)) {
            return;
        }

        if (!readBytes(&toc.index_offset, sizeof(uint64_t), offset, dataFile_)) {
            return;
        }
        if (!readBytes(&toc.summary_offset, sizeof(uint64_t), offset, dataFile_)) {
            return;
        }
        if (!readBytes(&toc.filter_offset, sizeof(uint64_t), offset, dataFile_)) {
            return;
        }
        if (!readBytes(&toc.meta_offset, sizeof(uint64_t), offset, dataFile_)) {
            return;
        }
            */

    readSummaryHeader();
}

bool SSTable::readBytes(void* dst, size_t n, uint64_t& offset, string fileName) const
{
    if (n == 0) return true;

    bool error = false;

    int block_id = offset / block_size;
    uint64_t block_pos = offset % block_size;

    vector<byte> block = bmp->read_block({ block_id++, fileName }, error);
    if (error) return !error;

    char* out = reinterpret_cast<char*>(dst);

    while (n > 0) {
        if (block_pos == block_size) {
            // fetch sledeci
            block = bmp->read_block({ block_id++, fileName }, error);
            if (error) return !error;

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