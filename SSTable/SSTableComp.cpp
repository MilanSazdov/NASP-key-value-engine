#include "SSTableComp.h"

SSTableComp::SSTableComp(const std::string& dataFile,
                         const std::string& indexFile,
                         const std::string& filterFile,
                         const std::string& summaryFile,
                         const std::string& metaFile,
                         Block_manager* bmp,
                         unordered_map<string, uint32_t>& map,
                         uint32_t& nextId
                        )
  : SSTable(dataFile, indexFile, filterFile, summaryFile, metaFile, bmp), mp(map), nextID(nextId)
{
}

void SSTableComp::build(std::vector<Record>& records)
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
vector<Record> SSTableComp::get(const std::string& key)
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
        uint64_t start = fileOffset;
        readNumValue(crc, fileOffset, dataFile_);
        uint64_t crc_size = fileOffset - start;

        char flag;
        readBytes(&flag, sizeof(flag), fileOffset, dataFile_);

        start = fileOffset;
        uint64_t ts = 0;
        readNumValue(ts, fileOffset, dataFile_);
        uint64_t ts_size = fileOffset - start;

        char tomb;
        readBytes(&tomb, sizeof(tomb), fileOffset, dataFile_);

        bool isTomb = (bool)tomb;

        uint64_t kSize = 0;
        readBytes(&kSize, sizeof(kSize), fileOffset, dataFile_);

        uint64_t vSize = 0;
        if(!isTomb) readBytes(&vSize, sizeof(vSize), fileOffset, dataFile_);

        std::string rkey;
        rkey.resize(kSize);
        readBytes(&rkey[0], kSize, fileOffset, dataFile_);

        std::string rvalue("");
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
                fileOffset += crc_size;
                
                char flag;
                readBytes(&flag, sizeof(flag), fileOffset, dataFile_);
                
                fileOffset += ts_size;
                
                fileOffset += sizeof(tomb);
                
                uint64_t kSize = 0;
                readBytes(&kSize, sizeof(kSize), fileOffset, dataFile_);
                
                uint64_t vSize = 0;
                if(isTomb)readBytes(&vSize, sizeof(vSize), fileOffset, dataFile_);
                
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
SSTableComp::writeDataMetaFiles(std::vector<Record>& sortedRecords)
{
    std::vector<IndexEntry> ret;
    ret.reserve(sortedRecords.size());


    // Merkle tree
    vector<string> leaves;
    std::hash<std::string> hasher;
    ull offset = 0ULL;

    int block_id = 0;

    string concat;

    // auto append_field = [&](const void* data, size_t len) {
    //     auto ptr = reinterpret_cast<const char*>(data);
    //     concat.append(ptr, len);
    // };

    
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
        
        Record rec(r);

        bool tomb = (bool)rec.tombstone;

        // Tomb se ne enkodira zato sto je 1 bajt, val i key size zbog splitovanja
        string crc = varenc::encodeVarint<uint>(rec.crc);
        string timestamp = varenc::encodeVarint<ull>(rec.timestamp);

        if (tomb) {
            rec.value_size = 0;
            rec.value = "";
        }

        // ---- Menjamo kljuc sa vrednoscu iz mape ---- //

        uint32_t key_val;
        auto it = mp.find(r.key);
        if (it == mp.end()) 
            mp[r.key] = nextID++;

        key_val = mp[r.key];

        string key_str = varenc::encodeVarint<uint32_t>(key_val);

        rec.key = key_str;
        rec.key_size = key_str.size();

        // -------------------------------------------- //

        size_t header_len =  tomb ? crc.size() + timestamp.size() :
                                    crc.size() + timestamp.size() + sizeof(rec.value_size);

        size_t len = header_len + rec.key_size + rec.value_size;

        offset += rec.key_size + rec.value_size; // Dodajemo ceo key size i value size prvo, posle cemo videti koliko headera treba
                            
        ull remaining = block_size - (offset % block_size);

        // Ako u bloku nema dovoljno mesta ni za record metadata (key uvek u prvom delu, bice < 32bit)
        if (remaining < header_len + rec.key_size) {
            offset += remaining;

            bmp->write_block({block_id++, dataFile_}, concat);
            concat.clear();
            remaining = block_size;
        }

        Wal_record_type flag;

        if (remaining < len) {
            flag = Wal_record_type::FIRST;
            concat.append(crc, crc.size());
            concat.append(reinterpret_cast<const char*>(&flag), sizeof(flag));
            concat.append(timestamp, timestamp.size());
            concat.append(reinterpret_cast<const char*>(&rec.tombstone), sizeof(rec.tombstone));

            remaining -= rec.key_size;
            
            ull value_written = min<ull>(remaining, rec.value_size);
            remaining -= value_written;
            
            if(!tomb) concat.append(reinterpret_cast<const char*>(&value_written), sizeof(value_written));
            
            remaining -= header_len;
            offset += header_len;

            concat.append(rec.key);
            concat.append(rec.value, value_written);

            // Flushujemo blok
            bmp->write_block({block_id++, dataFile_}, concat);
            concat.clear();

            rec.value = rec.value.substr(value_written);
            rec.value_size -= value_written;

            remaining = block_size;

            while (flag != Wal_record_type::LAST) {        
                remaining -= header_len;
                offset += header_len;

                ull value_written = min<ull>(remaining, rec.value_size);
                remaining -= value_written;

                if(remaining == 0 && (rec.value_size - value_written != 0)) {
                    flag = Wal_record_type::MIDDLE;
                } else flag = Wal_record_type::LAST;

                // L -> ili remaining != 0, tj ima jos mesta, ili nema vise mesta ali smo zapisali ceo zapis
                // M -> Nema vise mesta i ili nismo ispisali ceo kljuc ili nismo ispisali ceo value

                concat.append(crc, crc.size());
                concat.append(reinterpret_cast<const char*>(&flag), sizeof(flag));
                concat.append(timestamp, timestamp.size());
                concat.append(reinterpret_cast<const char*>(&rec.tombstone), sizeof(rec.tombstone));
                if(!tomb) concat.append(reinterpret_cast<const char*>(&value_written), sizeof(value_written));
                

                concat.append(rec.value, value_written);
                
                if (flag == Wal_record_type::MIDDLE){
                    // Flushujemo blok
                    bmp->write_block({block_id++, dataFile_}, concat);
                    concat.clear();

                    rec.value = string(rec.value.begin() + value_written, rec.value.end());

                    rec.value_size -= value_written;

                    remaining = block_size;
                }
            }

        } else {
            flag = Wal_record_type::FULL;
            concat.append(crc, crc.size());
            concat.append(reinterpret_cast<const char*>(&flag), sizeof(flag));
            concat.append(timestamp, timestamp.size());
            concat.append(reinterpret_cast<const char*>(&r.tombstone), sizeof(r.tombstone));
            if(!tomb) concat.append(reinterpret_cast<const char*>(&r.value_size), sizeof(r.value_size));

            offset += header_len;

            concat.append(r.key);
            if(!tomb) concat.append(r.value, r.value_size);    

        }
    }

    if (!concat.empty()) 
        bmp->write_block({block_id, dataFile_}, concat);


    buildMerkleTree(leaves);

    return ret;
}

std::vector<IndexEntry> SSTableComp::writeIndexToFile()
{
    std::vector<IndexEntry> ret;

    string payload;

    uint64_t count = index_.size();
    string count_str = varenc::encodeVarint<uint64_t>(count);
    payload.append(count_str);

    uint64_t offset = count_str.size();

    for (auto& ie : index_) {
        IndexEntry summEntry;
        summEntry.key = ie.key;
        summEntry.offset = offset;
        ret.push_back(summEntry);

        uint64_t k_size = ie.key.size();
        string kSize_str = varenc::encodeVarint<uint64_t>(k_size);
        string offset_str = varenc::encodeVarint<ull>(ie.offset);

        payload.append(kSize_str);
        payload.append(ie.key, k_size);
        payload.append(offset_str);

        offset += kSize_str.size() + k_size + offset_str.size();
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

void SSTableComp::writeSummaryToFile()
{
    string payload;

    string min_len_str = varenc::encodeVarint<size_t>(summary_.min.size());
    payload.append(min_len_str);
    string max_len_str = varenc::encodeVarint<size_t>(summary_.max.size());
    payload.append(max_len_str);
    string count = varenc::encodeVarint<size_t>(summary_.summary.size());
    payload.append(count);
    
    payload.append(summary_.min);
    payload.append(summary_.max);


    for (auto& summEntry : summary_.summary) {
        uint64_t k_size = summEntry.key.size();
        string k_size_str = varenc::encodeVarint<uint64_t>(k_size);
        string offset_str = varenc::encodeVarint<ull>(summEntry.offset);

        payload.append(k_size_str);
        payload.append(summEntry.key, k_size);
        payload.append(offset_str);

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

void SSTableComp::readMetaFromFile() {
    uint64_t offset = 0;
    bool error;

    uint64_t treeSize = 0;
    if (!readBytes(&treeSize, sizeof(treeSize), offset, metaFile_)) {
        std::cerr << "[SSTableComp::readMetaFromFile] readMetaFromFile: failed to read tree size\n";
        return;
    }

    tree.clear();
    tree.reserve(treeSize);

    for (uint64_t i = 0; i < treeSize; ++i) {
        std::string hashStr;
        hashStr.resize(sizeof(uint64_t));

        if (!readBytes(&hashStr[0], sizeof(uint64_t), offset, metaFile_)) {
            std::cerr << "[SSTableComp::readMetaFromFile] readMetaFromFile: failed to read hash #" << i << "\n";
            return;
        }

        tree.push_back(std::move(hashStr));
    }
}

void SSTableComp::writeBloomToFile() const
{
    std::vector<byte> raw = bloom_.serialize();

    string len_str = varenc::encodeVarint<size_t>(raw.size());

    string payload;

    payload.append(len_str);

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

void SSTableComp::writeMetaToFile() const
{
    string size_str = varenc::encodeVarint<size_t>(tree.size());

    string payload;

    payload.append(reinterpret_cast<const char*>(&size_str), sizeof(size_str));

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

void SSTableComp::readBloomFromFile()
{
    uint64_t fileOffset = 0;
    uint64_t len = 0;
    
    if (!readNumValue<uint64_t>(len, fileOffset, metaFile_)) {
        std::cerr << "[SSTableComp::readBloomFromFile] Problem reading bloom length\n";
        return;
    }

    if(len==0) return;

    std::vector<byte> raw(len);
    if (!readBytes(raw.data(), len, fileOffset, metaFile_)) {
        std::cerr << "[SSTableComp::readBloomFromFile] Problem reading bloomfilter\n";
        return;
    }

    bloom_ = BloomFilter::deserialize(raw);
}

void SSTableComp::readSummaryHeader()
{
    summary_.min.clear();
    summary_.max.clear();

    uint64_t offset = 0;

    uint64_t min_key_len = 0;
    if (!readNumValue<uint64_t>(min_key_len, offset, summaryFile_)) {
        std::cerr << "[SSTableComp::readSummaryHeader] Problem reading min_key_len\n";
        return;
    }

    uint64_t max_key_len = 0;
    if (!readNumValue<uint64_t>(max_key_len, offset, summaryFile_)) {
        std::cerr << "[SSTableComp::readSummaryHeader] Problem reading max_key_len\n";
        return;
    }

    if (!readNumValue<uint64_t>(summary_.count, offset, summaryFile_)) {
        std::cerr << "[SSTableComp::readSummaryHeader] Problem reading summary count\n";
        return;
    }

    summary_data_start = offset;

    summary_.min.reserve(min_key_len);
    if (!readBytes(&summary_.min, min_key_len, offset, summaryFile_)) {
        std::cerr << "[SSTableComp::readSummaryHeader] Problem reading summary_.min bytes\n";
        return;
    }

    summary_.max.reserve(max_key_len);
    if (!readBytes(&summary_.max, max_key_len, offset, summaryFile_)) {
        std::cerr << "[SSTableComp::readSummaryHeader] Problem reading summary_.max bytes\n";
        return;
    }
}

uint64_t SSTableComp::findDataOffset(const std::string& key, bool& found) const
{
    if (key < summary_.min || key > summary_.max) {
        found = false;
        return 0;
    }

    uint64_t fileOffset = summary_data_start;

    fileOffset += summary_.min.size() + summary_.max.size();
    
    uint64_t kSize;
    bool error;

    uint64_t lastOffset = 0;
    
    string rKey;
    for(int i = 0; i < summary_.count; i++){
        readNumValue<uint64_t>(kSize, fileOffset, summaryFile_);

        rKey.resize(kSize);
        readBytes(&rKey[0], kSize, fileOffset, summaryFile_);

        if(rKey > key) {
            break;
        }

        readNumValue<uint64_t>(lastOffset, fileOffset, summaryFile_);
    }

    fileOffset = lastOffset;
    uint64_t off = 0;

    while (true) {
        readNumValue<uint64_t>(kSize, fileOffset, indexFile_);
        
        rKey.resize(kSize);
        readBytes(&rKey[0], kSize, fileOffset, indexFile_);

        readNumValue<uint64_t>(off, fileOffset, indexFile_);

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

template<typename UInt>
bool SSTableComp::readNumValue(UInt& dst, uint64_t& fileOffset, string fileName) const {
    char chunk;
    bool finished=false;
    int val_offset = 0;

    dst = 0;

    do {
        if (!readBytes(chunk&, 1, fileOffset, fileName)) {
            return false;
        };
        finished = varenc::decodeVarint(chunk, dst, val_offset);
    } while (finished != true);
    return true;
}
