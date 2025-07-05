#include "SSTableRaw.h"
#include "VarEncoding.h"

SSTableRaw::SSTableRaw(const std::string& dataFile,
                       const std::string& indexFile,
                       const std::string& filterFile,
                       const std::string& summaryFile,
                       const std::string& metaFile,
                       Block_manager* bmp)
  : SSTable(dataFile, indexFile, filterFile, summaryFile, metaFile, bmp)
{
}

void SSTableRaw::build(std::vector<Record>& records)
{
    if (records.empty()) {
        std::cerr << "[SSTable] build: Nema zapisa.\n";
        return;
    }

    index_ = writeDataMetaFiles(records);

    BloomFilter bf(records.size(), 0.01); // TODO: Config
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

    std::cout << "[SSTable] build: upisano " << records.size()
        << " zapisa u " << dataFile_ << ".\n";
}


// Funkcija NE PROVERAVA bloomfilter, pretpostavlja da je vec provereno
vector<Record> SSTableRaw::get(const std::string& key)
{
    readSummaryHeader();
    vector<Record> matches;

    // 3) binarna pretraga -> offset
    bool found;
    uint64_t fileOffset = findDataOffset(key, found);
    if (!found)
    {
        return matches;
    }

    // 4) Otvorimo dataFile, idemo od fileOffset redom
    while (true) {
        if(block_size - (fileOffset % block_size) < 30) {
            fileOffset += block_size - (fileOffset % block_size);
        } // Ako u bloku posle recorda nema mesta za header, znaci da smo padovali i sledeci record pocinje u sledecem bloku

        // citamo polja Record-a
        uint32_t crc = 0;
        readBytes(&crc, sizeof(crc), fileOffset, dataFile_);

        char flag;
        readBytes(&flag, sizeof(flag), fileOffset, dataFile_);

        uint64_t ts = 0;
        readBytes(&ts, sizeof(ts), fileOffset, dataFile_);

        char tomb;
        readBytes(&tomb, sizeof(tomb), fileOffset, dataFile_);

        uint64_t kSize = 0;
        readBytes(&kSize, sizeof(kSize), fileOffset, dataFile_);

        uint64_t vSize = 0;
        readBytes(&vSize, sizeof(vSize), fileOffset, dataFile_);

        std::string rkey;
        rkey.resize(kSize);
        readBytes(&rkey[0], kSize, fileOffset, dataFile_);

        std::string rvalue;
        rvalue.resize(vSize);
        readBytes(&rvalue[0], vSize, fileOffset, dataFile_);

        Record r;
        r.crc = crc;
        r.timestamp = ts;
        r.tombstone = static_cast<byte>(tomb);
        r.key_size = kSize;
        r.value_size = vSize;
        r.key = rkey;
        r.value = rvalue;

        
        if (flag == 'F') {
            while(true) {
                fileOffset += sizeof(crc);
                
                char flag;
                readBytes(&flag, sizeof(flag), fileOffset, dataFile_);
                
                fileOffset += sizeof(ts);
                
                fileOffset += sizeof(tomb);
                
                uint64_t kSize = 0;
                readBytes(&kSize, sizeof(kSize), fileOffset, dataFile_);
                
                uint64_t vSize = 0;
                readBytes(&vSize, sizeof(vSize), fileOffset, dataFile_);
                
                std::string rkey;
                rkey.resize(kSize);
                readBytes(&rkey[0], kSize, fileOffset, dataFile_);
                
                std::string rvalue;
                rvalue.resize(vSize);
                readBytes(&rvalue[0], vSize, fileOffset, dataFile_);
                
                r.value.append(rvalue);
                r.key.append(rkey);
                r.key_size += kSize;
                r.value_size += vSize;
                
                if(flag == 'L'){
                    break;
                }
            }
        }
        
        if(r.key == key) {
            matches.emplace_back(r);
        }
        
        if (r.key > key) {
            // nema smisla ici dalje, data fajl je sortiran
            return matches;
        }
    }
    return matches;
}

/*
std::vector<std::pair<std::string, std::string>>
SSTable::range_scan(const std::string& startKey, const std::string& endKey)
{
    std::vector<std::pair<std::string, std::string>> result;

    readBloomFromFile();
    readIndexFromFile();

    // pocetni offset
    uint64_t offset = findDataOffset(startKey);

    std::ifstream in(dataFile_, std::ios::binary);
    if (!in.is_open()) {
        return result;
    }
    in.seekg(offset, std::ios::beg);

    while (true) {
        uint32_t crc;
        if (!in.read(reinterpret_cast<char*>(&crc), sizeof(crc))) break;
        char flag;
        if (!in.read(reinterpret_cast<char*>(&flag), sizeof(flag))) break;
        uint64_t ts;
        if (!in.read(reinterpret_cast<char*>(&ts), sizeof(ts))) break;
        char tomb;
        if (!in.read(reinterpret_cast<char*>(&tomb), sizeof(tomb))) break;
        uint64_t kSize;
        if (!in.read(reinterpret_cast<char*>(&kSize), sizeof(kSize))) break;
        uint64_t vSize;
        if (!in.read(reinterpret_cast<char*>(&vSize), sizeof(vSize))) break;

        std::string rkey(kSize, '\0');
        if (!in.read(&rkey[0], kSize)) break;
        std::string rvalue(vSize, '\0');
        if (!in.read(&rvalue[0], vSize)) break;

        if (rkey > endKey) {
            break;
        }
        if (rkey >= startKey && rkey <= endKey && tomb == 0) {
            result.push_back({ rkey, rvalue });
        }
    }

    return result;
}
*/


std::vector<IndexEntry>
SSTableRaw::writeDataMetaFiles(std::vector<Record>& sortedRecords)
{
    std::vector<IndexEntry> ret;
    ret.reserve(sortedRecords.size());


    // Merkle tree
    vector<string> leaves;
    std::hash<std::string> hasher;
    ull offset = 0ULL;

    int block_id = 0;

    string concat;

    auto append_field = [&](const void* data, size_t len) {
        auto ptr = reinterpret_cast<const char*>(data);
        concat.append(ptr, len);
    };

   size_t header_len = sizeof(uint) + 2*sizeof(byte) + 3*sizeof(ull);

    for (auto& r : sortedRecords) {
        { // Deo za merkle tree
            uint64_t h = hasher(r.key + r.value);
            string leaf;
            leaf.resize(sizeof(h));
            memcpy(&leaf[0], &h, sizeof(h));
            leaves.push_back(move(leaf));
        }

        IndexEntry ie;
        ie.key = r.key;
        ie.offset = offset;
        ret.emplace_back(ie);


        size_t len = header_len + r.key_size + r.value_size;

        ull remaining = block_size - (offset % block_size);

        offset += r.key_size + r.value_size; // Dodajemo ceo key size i value size prvo, posle cemo videti koliko headera treba
        
        // Ako u bloku nema dovoljno mesta ni za record metadata
        if (remaining < header_len) {
            // concat.insert(concat.end(), remaining, (byte)0); write_block valjda vec paduje
            offset += remaining;

            bmp->write_block({block_id++, dataFile_}, concat);
            concat.clear();
            remaining = block_size;
        }

        Wal_record_type flag;

        if (remaining < len) {
            flag = Wal_record_type::FIRST;
            Record rec(r);
            append_field(&rec.crc, sizeof(rec.crc));
            append_field(&flag, sizeof(flag));
            append_field(&rec.timestamp, sizeof(rec.timestamp));
            append_field(&rec.tombstone, sizeof(rec.tombstone));

            remaining -= header_len;

            ull key_written = min<ull>(remaining, rec.key_size);
            remaining -= key_written;

            ull value_written = min<ull>(remaining, rec.value_size);
            remaining -= value_written;

            concat.append(
            reinterpret_cast<const char*>(&key_written),
            sizeof(key_written)
            );


            concat.append(
            reinterpret_cast<const char*>(&value_written),
            sizeof(value_written)
            );

            
            offset += header_len;

            concat.insert(concat.end(), rec.key.begin(), rec.key.begin()+key_written);
            concat.insert(concat.end(), rec.value.begin(), rec.value.begin()+value_written);
 

            // Flushujemo blok
            bmp->write_block({block_id++, dataFile_}, concat);
            concat.clear();

            rec.key = rec.key.substr(key_written);
            rec.value = rec.value.substr(value_written);

            rec.key_size -= key_written;
            rec.value_size -= value_written;

            remaining = block_size;

            while (flag != Wal_record_type::LAST) {
                remaining -= header_len;

                ull key_written = min<ull>(remaining, rec.key_size);
                remaining -= key_written;
                ull value_written = min<ull>(remaining, rec.value_size);
                remaining -= value_written;

                if(remaining == 0 && (rec.key_size - key_written != 0 || rec.value_size - value_written != 0)) {
                    flag = Wal_record_type::MIDDLE;
                } else flag = Wal_record_type::LAST;

                // L -> ili remaining != 0, tj ima jos mesta, ili nema vise mesta ali smo zapisali ceo zapis
                // M -> Nema vise mesta i ili nismo ispisali ceo kljuc ili nismo ispisali ceo value

                append_field(&rec.crc, sizeof(rec.crc));
                append_field(&flag, sizeof(flag));
                append_field(&rec.timestamp, sizeof(rec.timestamp));
                append_field(&rec.tombstone, sizeof(rec.tombstone));

                offset += header_len;

                concat.append(
                    reinterpret_cast<const char*>(&key_written),
                    sizeof(key_written)
                );

                concat.append(
                    reinterpret_cast<const char*>(&value_written),
                    sizeof(value_written)
                );

                concat.insert(concat.end(), rec.key.begin(), rec.key.begin()+key_written);
                concat.insert(concat.end(), rec.value.begin(), rec.value.begin()+value_written);
                
                if (flag == Wal_record_type::MIDDLE){
                    // Flushujemo blok
                    bmp->write_block({block_id++, dataFile_}, concat);
                    concat.clear();


                    rec.key = string(rec.key.begin() + key_written, rec.key.end());
                    rec.value = string(rec.value.begin() + value_written, rec.value.end());

                    rec.key_size -= key_written;
                    rec.value_size -= value_written;

                    remaining = block_size;
                }

            }

        } else {
            flag = Wal_record_type::FULL;
            append_field(&r.crc, sizeof(r.crc));
            append_field(&flag, sizeof(flag));
            append_field(&r.timestamp, sizeof(r.timestamp));
            append_field(&r.tombstone, sizeof(r.tombstone));
            append_field(&r.key_size, sizeof(r.key_size));
            append_field(&r.value_size, sizeof(r.value_size));

            offset += header_len;

            concat.insert(concat.end(), r.key.begin(), r.key.begin()+r.key_size);
            concat.insert(concat.end(), r.value.begin(), r.value.begin()+r.value_size);    

        }
    }

    if (!concat.empty()) 
        bmp->write_block({block_id, dataFile_}, concat);


    buildMerkleTree(leaves);

    return ret;
}

std::vector<IndexEntry> SSTableRaw::writeIndexToFile()
{
    std::vector<IndexEntry> ret;

    string payload;

    uint64_t count = index_.size();
    payload.append(reinterpret_cast<char*>(&count), sizeof(count));

    uint64_t offset = 8ULL;

    for (auto& ie : index_) {
        IndexEntry summEntry;
        summEntry.key = ie.key;
        summEntry.offset = offset;
        ret.push_back(summEntry);

        uint64_t kSize = ie.key.size();
        payload.append(reinterpret_cast<char*>(&kSize), sizeof(kSize));
        payload.append(ie.key.data(), kSize);
        payload.append(reinterpret_cast<char*>(&ie.offset), sizeof(ie.offset));

        offset += 8 + kSize + 8;
    }

    int block_id = 0;
    size_t total_bytes = payload.size();
    offset = 0;

    while (offset + block_size <= total_bytes) {
        string chunk = payload.substr(offset, block_size);
        bmp->write_block({block_id++, indexFile_}, chunk);
        offset += block_size;
    }

    return ret;
}

void SSTableRaw::writeSummaryToFile()
{
    string payload;

    uint64_t minLen = summary_.min.size();
    payload.append(reinterpret_cast<const char*>(&minLen), sizeof(minLen));
    uint64_t maxLen = summary_.max.size();
    payload.append(reinterpret_cast<const char*>(&maxLen), sizeof(maxLen));
    uint64_t count = summary_.summary.size();
    payload.append(reinterpret_cast<const char*>(&count), sizeof(count));
    
    payload.append(summary_.min);
    payload.append(summary_.max);


    for (auto& summEntry : summary_.summary) {
        uint64_t kSize = summEntry.key.size();
        payload.append(reinterpret_cast<char*>(&kSize), sizeof(kSize));
        payload.append(summEntry.key.data(), kSize);
        payload.append(reinterpret_cast<char*>(&summEntry.offset), sizeof(summEntry.offset));

    }

    int block_id = 0;
    uint64_t total_bytes = payload.size();
    uint64_t offset = 0;

    while (offset + block_size <= total_bytes) {
        string chunk = payload.substr(offset, block_size);
        bmp->write_block({block_id++, summaryFile_}, chunk);
        offset += block_size;
    }

    if (offset < total_bytes) {
        std::string chunk = payload.substr(offset);
        bmp->write_block({ block_id++, summaryFile_ }, chunk);
    }

}

void SSTableRaw::readMetaFromFile() {
    uint64_t offset = 0;
    bool error;

    uint64_t treeSize = 0;
    if (!readBytes(&treeSize, sizeof(treeSize), offset, metaFile_)) {
        std::cerr << "[SSTable] readMetaFromFile: failed to read tree size\n";
        return;
    }

    tree.clear();
    tree.reserve(treeSize);

    for (uint64_t i = 0; i < treeSize; ++i) {
        std::string hashStr;
        hashStr.resize(sizeof(uint64_t));

        if (!readBytes(&hashStr[0], sizeof(uint64_t), offset, metaFile_)) {
            std::cerr << "[SSTable] readMetaFromFile: failed to read hash #" << i << "\n";
            return;
        }

        tree.push_back(std::move(hashStr));
    }
}

void SSTableRaw::writeBloomToFile() const
{
    std::vector<byte> raw = bloom_.serialize();

    uint64_t len = raw.size();

    string payload;

    payload.append(reinterpret_cast<const char*>(&len), sizeof(len));

    for (const auto& i : raw) {
        payload.append(reinterpret_cast<const char*>(i), sizeof(i));
    }

    int block_id = 0;
    size_t total_bytes = payload.size();
    size_t offset = 0;

    while (offset + block_size <= total_bytes) {
        string chunk = payload.substr(offset, block_size);
        bmp->write_block({block_id++, summaryFile_}, chunk);
        offset += block_size;
    }
}

void SSTableRaw::writeMetaToFile() const
{
    uint64_t treeSize = tree.size();

    string payload;

    payload.append(reinterpret_cast<const char*>(&treeSize), sizeof(treeSize));

    for (const auto& hashStr : tree) {
        if (hashStr.size() != sizeof(uint64_t)) {
            std::cerr << "[SSTable] writeMetaToFile: Ocekuju se vrednosti duzine 8 byte, a duzina hash-a je " + to_string(hashStr.size()) + " bajtova." << std::endl;
            return;
        }
        payload.append(hashStr.data(), sizeof(uint64_t));
    }

    int block_id = 0;
    size_t total_bytes = payload.size();
    size_t offset = 0;

    while (offset + block_size <= total_bytes) {
        string chunk = payload.substr(offset, block_size);
        bmp->write_block({block_id++, summaryFile_}, chunk);
        offset += block_size;
    }
}

void SSTableRaw::readBloomFromFile()
{
    uint64_t fileOffset = 0;

    uint64_t len = 0;
    if (!readBytes(&len, sizeof(len), fileOffset, metaFile_) || len == 0) {
        return;
    }

    std::vector<byte> raw(len);
    if (!readBytes(raw.data(), len, fileOffset, metaFile_)) {
        return;
    }

    bloom_ = BloomFilter::deserialize(raw);
}

void SSTableRaw::readSummaryHeader()
{
    summary_.min.clear();
    summary_.max.clear();
    bool err;
    
    int block_id = 0;
    int offset = 0;
    
    vector<byte> block = bmp->read_block({block_id, summaryFile_}, err);

    uint64_t minKeyLength = 0;
    memcpy(&minKeyLength, block.data(), sizeof(minKeyLength));

    offset += sizeof(minKeyLength);

    uint64_t maxKeyLength = 0; 
    memcpy(&maxKeyLength, block.data()+offset, sizeof(maxKeyLength));
    offset += sizeof(maxKeyLength);

    memcpy(&summary_.count, block.data()+offset, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    summary_.min.reserve(minKeyLength);

    size_t fileOffset = offset;
    size_t remaining  = minKeyLength;

    while (remaining > 0) {
        int neededBlock = fileOffset / block_size;
        size_t block_offset = fileOffset % block_size;

        if (neededBlock != block_id) {
            block_id = neededBlock;
            block = bmp->read_block({block_id, summaryFile_}, err);
        }

        size_t take = std::min(remaining, block_size - block_offset);

        summary_.min.append(reinterpret_cast<const char*>(block.data() + block_offset), take);

        fileOffset += take;
        remaining  -= take;
    }

    summary_.max.reserve(maxKeyLength);

    offset = fileOffset;
    remaining = maxKeyLength;
    while (remaining > 0) {
            int neededBlock = fileOffset / block_size;
            size_t block_offset = fileOffset % block_size;

            if (neededBlock != block_id) {
                block_id = neededBlock;
                block = bmp->read_block({block_id, summaryFile_}, err);
            }

            size_t take = std::min(remaining, block_size - block_offset);

            summary_.max.append(reinterpret_cast<const char*>(block.data() + block_offset), take);

            fileOffset += take;
            remaining  -= take;
        }
}

uint64_t SSTableRaw::findDataOffset(const std::string& key, bool& found) const
{
    if (key < summary_.min || key > summary_.max) {
        found = false;
        return 0;
    }

    size_t fileOffset = summary_.min.size() + summary_.max.size() + 3*sizeof(uint64_t);
    
    uint64_t kSize;
    bool error;

    uint64_t lastOffset = 0ULL;
    
    string rKey;
    for(int i = 0; i < summary_.count; i++){
        readBytes(&kSize, sizeof(kSize), fileOffset, summaryFile_);

        rKey.resize(kSize);
        readBytes(&rKey[0], kSize, fileOffset, summaryFile_);

        if(rKey > key) {
            break;
        }

        readBytes(&lastOffset, sizeof(lastOffset), fileOffset, summaryFile_);
    }

    // works up until this point
    fileOffset = lastOffset;
    uint64_t off;

    while (true) {
        readBytes(&kSize, sizeof(kSize), fileOffset, indexFile_);
        
        rKey.resize(kSize);
        readBytes(&rKey[0], kSize, fileOffset, indexFile_);

        readBytes(&off, sizeof(off), fileOffset, indexFile_);

        if (rKey == key) {
            found = true;
            return off;
        }
        if (rKey > key) {
            found = false;
            return 0ULL;
        }
    }
    found = false;
    return 0ULL;
}

