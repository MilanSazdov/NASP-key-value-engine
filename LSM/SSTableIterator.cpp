#include "SSTableIterator.h"
#include <filesystem>
#include <algorithm>
#include <iostream>

namespace fs = std::filesystem;

SSTableIterator::SSTableIterator(const SSTableMetadata& meta, size_t block_size)
    : meta_(meta),
    bm_(std::make_unique<Block_manager>()),
    block_size_(block_size),
    current_offset_(0) {
    try {
        if (fs::exists(meta_.data_path)) {
            file_size_ = fs::file_size(meta_.data_path);
        }
        else {
            file_size_ = 0;
        }
    }
    catch (const fs::filesystem_error& e) {
        std::cerr << "Error getting file size for " << meta_.data_path << ": " << e.what() << std::endl;
        file_size_ = 0;
    }

    // odmah procitaj prvi zapis ako postoji
    if (hasNext()) {
        try {
            next_record_ = readNextRecord();
        }
        catch (const std::exception& e) {
            std::cerr << "Error reading first record from " << meta_.data_path << ": " << e.what() << std::endl;
            file_size_ = 0; // smatraj iterator nevazecim
            next_record_.reset();
        }
    }
}

SSTableIterator::~SSTableIterator() = default;

bool SSTableIterator::hasNext() const {
    return current_offset_ < file_size_;
}

Record SSTableIterator::next() {
    if (!next_record_.has_value()) {
        throw std::out_of_range("SSTableIterator::next() called on invalid iterator");
    }

    Record current = std::move(next_record_.value());

    if (hasNext()) {
        try {
            next_record_ = readNextRecord();
        }
        catch (const std::exception& e) {
            std::cerr << "Error reading next record from " << meta_.data_path << ": " << e.what() << std::endl;
            file_size_ = current_offset_; // smatraj iterator zavrsenim
            next_record_.reset();
        }
    }
    else {
        next_record_.reset();
    }

    return current;
}

// pomocna funkcija za citanje bajtova
bool SSTableIterator::readBytes(void* dst, size_t n) {
    if (current_offset_ + n > file_size_) {
        return false; // pokušaj citanja preko kraja fajla
    }

    bool error = false;
    char* out = reinterpret_cast<char*>(dst);
    uint64_t read_offset = current_offset_;

    while (n > 0) {
        int block_id = read_offset / block_size_;
        uint64_t block_pos = read_offset % block_size_;

        std::vector<byte> block = bm_->read_block({ block_id, meta_.data_path }, error);
        if (error) return false;

        size_t take = std::min(n, block_size_ - block_pos);
        if (block.size() < block_pos + take) {
            return false; // Blok je manji nego sto se ocekuje
        }

        std::memcpy(out, block.data() + block_pos, take);

        out += take;
        read_offset += take;
        n -= take;
    }
    current_offset_ = read_offset;
    return true;
}

// glavna logika za citanje jednog (potencijalno podeljenog) zapisa
Record SSTableIterator::readNextRecord() {
    Record record;
    Wal_record_type flag;

    // Procitaj prvi deo zapisa
    if (!readBytes(&record.crc, sizeof(record.crc))) throw std::runtime_error("Failed to read CRC");
    if (!readBytes(&flag, sizeof(flag))) throw std::runtime_error("Failed to read flag");
    if (!readBytes(&record.timestamp, sizeof(record.timestamp))) throw std::runtime_error("Failed to read timestamp");
    if (!readBytes(&record.tombstone, sizeof(record.tombstone))) throw std::runtime_error("Failed to read tombstone");

    uint64_t kSize, vSize;
    if (!readBytes(&kSize, sizeof(kSize))) throw std::runtime_error("Failed to read key size");
    if (!readBytes(&vSize, sizeof(vSize))) throw std::runtime_error("Failed to read value size");

    record.key.resize(kSize);
    record.value.resize(vSize);
    if (!readBytes(record.key.data(), kSize)) throw std::runtime_error("Failed to read key data");
    if (!readBytes(record.value.data(), vSize)) throw std::runtime_error("Failed to read value data");

    // Ako je zapis podeljen, nastavi sa citanjem delova
    while (flag == Wal_record_type::FIRST || flag == Wal_record_type::MIDDLE) {
        uint32_t next_crc;
        Wal_record_type next_flag;
        uint64_t next_ts, next_kSize, next_vSize;
        byte next_tomb;

        if (!readBytes(&next_crc, sizeof(next_crc))) break;
        if (!readBytes(&next_flag, sizeof(next_flag))) break;
        if (!readBytes(&next_ts, sizeof(next_ts))) break;
        if (!readBytes(&next_tomb, sizeof(next_tomb))) break;
        if (!readBytes(&next_kSize, sizeof(next_kSize))) break;
        if (!readBytes(&next_vSize, sizeof(next_vSize))) break;

        size_t old_key_size = record.key.size();
        size_t old_value_size = record.value.size();
        record.key.resize(old_key_size + next_kSize);
        record.value.resize(old_value_size + next_vSize);

        if (!readBytes(record.key.data() + old_key_size, next_kSize)) break;
        if (!readBytes(record.value.data() + old_value_size, next_vSize)) break;

        flag = next_flag;
    }

    record.key_size = record.key.size();
    record.value_size = record.value.size();

    return record;
}