#pragma once

#include <string>
#include <memory>
#include <optional>
#include "SSTable.h"
#include "Compaction.h"

// Forward-declare Block_manager da izbegnemo circular dependency ako je u.cpp
class Block_manager;

class SSTableIterator {
public:
    SSTableIterator(const SSTableMetadata& meta, size_t block_size);
    ~SSTableIterator();

    bool hasNext() const;
    Record next();

private:
    Record readNextRecord();
    bool readBytes(void* dst, size_t n);

    SSTableMetadata meta_;
    std::unique_ptr<Block_manager> bm_;
    size_t block_size_;
    uint64_t current_offset_;
    uint64_t file_size_;
    std::optional<Record> next_record_;
};

