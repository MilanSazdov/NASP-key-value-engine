#include "SSTableComp.h"
#include <filesystem>
#include "../Utils/VarEncoding.h"
#include "SSTable.h"

SSTableComp::SSTableComp(const std::string & dataFile,
    const std::string & indexFile,
    const std::string & filterFile,
    const std::string & summaryFile,
    const std::string & metaFile,
    Block_manager * bmp,
    unordered_map<string, uint32_t>&key_to_id,
    vector<string>&id_to_key,
    uint32_t & nextId)
    : SSTable(dataFile, indexFile, filterFile, summaryFile, metaFile, bmp),
    key_to_id(key_to_id),
    id_to_key(id_to_key),
    nextID(nextId)
{
}

SSTableComp::SSTableComp(const std::string& dataFile,
    Block_manager* bmp,
    unordered_map<string, uint32_t>& map,
    vector<string>& id_to_key,
    uint32_t& nextId)
    : SSTable(dataFile, bmp),
    key_to_id(map),
    id_to_key(id_to_key),
    nextID(nextId)
{
} // single_file_mode


vector<Record> SSTableComp::get(const std::string& key)
{
    prepare(); // Citamo TOC (sparsity, offsete, itd.) i header summary fajla
    vector<Record> matches;

    // pretraga -> offset rekorda
    bool in_file;
    uint64_t fileOffset = findRecordOffset(key, in_file);
    if (!in_file)
    {
        return matches;
    }

    while (true) {
        // EOF
        // nikada da se desi zato sto proveravamo
        // da li je kljuc koji se trazi veci od summary.max_key
        if (fileOffset >= toc.data_end || fileOffset < toc.data_offset) break;

        if (block_size - (fileOffset % block_size) <
            sizeof(uint) + sizeof(ull) + 1 + 1 + sizeof(uint64_t) + sizeof(uint32_t)) {
            fileOffset += block_size - (fileOffset % block_size);
        } // Ako nema mesta za header u najgorem slucaju, paddujemo i to treba da se preskoci.

        // citamo polja Record-a
        uint crc = 0;
        uint64_t start = fileOffset;
        readNumValue<uint>(crc, fileOffset, dataFile_);
        uint64_t crc_size = fileOffset - start;


        Wal_record_type flag;
        readBytes(&flag, sizeof(flag), fileOffset, dataFile_);

        start = fileOffset;
        uint64_t ts = 0;
        readNumValue(ts, fileOffset, dataFile_);
        uint64_t ts_size = fileOffset - start;

        char tomb;
        readBytes(&tomb, sizeof(tomb), fileOffset, dataFile_);

        bool isTomb = (bool)tomb;

        uint64_t v_size = 0;
        if (!isTomb) readBytes(&v_size, sizeof(v_size), fileOffset, dataFile_);

        start = fileOffset;
        uint32_t r_key_id = 0;
        readNumValue(r_key_id, fileOffset, dataFile_);
        uint64_t r_key_size = fileOffset - start;

        string rkey = id_to_key[r_key_id];

        std::string rvalue;
        rvalue.resize(v_size);
        readBytes(&rvalue[0], v_size, fileOffset, dataFile_);

        Record r;
        r.crc = crc;
        r.timestamp = ts;
        r.tombstone = static_cast<byte>(tomb);
        r.key_size = rkey.size();
        r.value_size = v_size;
        r.key = rkey;
        r.value = rvalue;

        if (rkey == key) {
            if (flag == Wal_record_type::FIRST) {
                while (true) {
                    fileOffset += crc_size;

                    Wal_record_type flag;
                    readBytes(&flag, sizeof(flag), fileOffset, dataFile_);

                    fileOffset += ts_size;

                    fileOffset += sizeof(tomb);

                    uint64_t vSize = 0;
                    if (!isTomb) readBytes(&vSize, sizeof(vSize), fileOffset, dataFile_);

                    std::string rvalue;
                    rvalue.resize(vSize);
                    readBytes(&rvalue[0], vSize, fileOffset, dataFile_);

                    r.value.append(rvalue);
                    r.value_size += vSize;

                    if (flag == Wal_record_type::LAST) {
                        break;
                    }
                }
            }
            matches.emplace_back(r);
        }

        if (r.key > key) {
            // nema smisla ici dalje, data fajl je sortiran
            return matches;
        }
    }
    return matches;
}

// Struktura: [crc(varInt), flag(byte), timestamp(varInt), tombstone(byte), val_size(uint64), key_id(varInt), value(string)]
// Kada se splituje, samo FIRST deo ima key_id
std::vector<IndexEntry>
SSTableComp::writeDataMetaFiles(std::vector<Record>& sortedRecords) {

    toc.data_offset = (sizeof(toc) / block_size + 1) * block_size; // Mesto za toc


    std::vector<IndexEntry> ret;
    ret.reserve(sortedRecords.size());


    ull offset = toc.data_offset;

    int block_id = toc.data_offset / block_size;

    string concat;
    concat.reserve(block_size);

    std::vector<std::string> data_for_merkle;
    data_for_merkle.reserve(sortedRecords.size());

    for (auto& r : sortedRecords) {

        {   // Merkle tree listovi
            data_for_merkle.push_back(r.key + r.value);
        }

        IndexEntry ie;
        ie.key = r.key;
        ie.offset = offset;
        ret.emplace_back(ie);

        Record rec(r);

        bool tomb = (bool)rec.tombstone;

        // Tomb se ne enkodira zato sto je 1 bajt, val size zbog splitovanja
        std::string crc = varenc::encodeVarint<uint>(rec.crc);
        std::string timestamp = varenc::encodeVarint<ull>(rec.timestamp);

        if (tomb) {
            rec.value_size = 0;
            rec.value = "";
        }

        // ---- Menjamo kljuc sa vrednoscu iz mape ---- //

        uint32_t key_val;
        auto it = key_to_id.find(r.key);
        if (it == key_to_id.end()) {
            key_to_id[r.key] = nextID++;
            id_to_key.emplace_back(r.key);
        }

        key_val = key_to_id[r.key];

        string key_str = varenc::encodeVarint<uint32_t>(key_val);

        rec.key = key_str;
        rec.key_size = key_str.size();

        // -------------------------------------------- //

        size_t header_len = tomb ? crc.size() + timestamp.size() + 2 :
            crc.size() + timestamp.size() + sizeof(rec.value_size) + 2;

        size_t len = header_len + rec.key_size + rec.value_size;

        ull remaining = block_size - (offset % block_size);

        offset += rec.key_size + rec.value_size; // Dodajemo ceo key size i value size prvo, posle cemo videti koliko headera treba

        // Ako u bloku nema dovoljno mesta za worst case header (worst case zbog citanja)
        uint64_t header_max_len = sizeof(r.crc) + sizeof(r.timestamp) +
            sizeof(char) + sizeof(r.tombstone) +
            sizeof(r.value_size) + /*najduzi kljuc:*/ sizeof(uint32_t);
        if (remaining < header_max_len)
        {
            offset += remaining;

            bmp->write_block({ block_id++, dataFile_ }, concat);
            concat.clear();
            remaining = block_size;
        }

        Wal_record_type flag;

        if (remaining < len) {
            flag = Wal_record_type::FIRST;
            concat.append(crc);
            concat.append(reinterpret_cast<const char*>(&flag), sizeof(flag));
            concat.append(timestamp);
            concat.append(reinterpret_cast<const char*>(&rec.tombstone), sizeof(rec.tombstone));

            remaining -= header_len;
            remaining -= rec.key_size;

            ull value_written = min<ull>(remaining, rec.value_size);
            remaining -= value_written;

            if (!tomb) concat.append(reinterpret_cast<const char*>(&value_written), sizeof(value_written));

            offset += header_len;

            concat.append(rec.key);
            concat.insert(concat.end(), rec.value.begin(), rec.value.begin() + value_written);

            rec.value = rec.value.substr(value_written);
            rec.value_size -= value_written;

            // Flushujemo blok
            bmp->write_block({ block_id++, dataFile_ }, concat);
            concat.clear();
            concat.reserve(block_size);

            remaining = block_size;

            while (flag != Wal_record_type::LAST) {
                remaining -= header_len;
                offset += header_len;

                ull value_written = min<ull>(remaining, rec.value_size);
                remaining -= value_written;

                if (remaining == 0 && (rec.value_size - value_written != 0)) {
                    flag = Wal_record_type::MIDDLE;
                }
                else flag = Wal_record_type::LAST;

                // L -> ili remaining != 0, tj ima jos mesta, ili nema vise mesta ali smo zapisali ceo zapis
                // M -> Nema vise mesta i ili nismo ispisali ceo kljuc ili nismo ispisali ceo value

                concat.append(crc);
                concat.append(reinterpret_cast<const char*>(&flag), sizeof(flag));
                concat.append(timestamp);
                concat.append(reinterpret_cast<const char*>(&rec.tombstone), sizeof(rec.tombstone));
                if (!tomb) concat.append(reinterpret_cast<const char*>(&value_written), sizeof(value_written));


                concat.insert(concat.end(), rec.value.begin(), rec.value.begin() + value_written);

                if (flag == Wal_record_type::MIDDLE) {
                    // Flushujemo blok
                    bmp->write_block({ block_id++, dataFile_ }, concat);
                    concat.clear();
                    concat.reserve(block_size);

                    rec.value = string(rec.value.begin() + value_written, rec.value.end());

                    rec.value_size -= value_written;

                    remaining = block_size;
                }
            }

        }
        else {
            flag = Wal_record_type::FULL;
            concat.append(crc);
            concat.append(reinterpret_cast<const char*>(&flag), sizeof(flag));
            concat.append(timestamp);
            concat.append(reinterpret_cast<const char*>(&rec.tombstone), sizeof(rec.tombstone));
            if (!tomb) concat.append(reinterpret_cast<const char*>(&rec.value_size), sizeof(rec.value_size));

            offset += header_len;

            concat.append(rec.key);
            if (!tomb) concat.append(rec.value);

            // Flush
            if(remaining==len){
                bmp->write_block({block_id++, dataFile_}, concat);
                concat.clear();
            }
        }
    }

    if (!concat.empty())
        bmp->write_block({ block_id, dataFile_ }, concat);

    toc.data_end = block_id * block_size + concat.size();
    if (is_single_file_mode_) toc.index_offset = (block_id + 1) * block_size;

    if (!data_for_merkle.empty()) {
        MerkleTree merkle_tree(data_for_merkle);
        this->rootHash_ = merkle_tree.getRootHash();
        this->originalLeafHashes_ = merkle_tree.getLeaves();
    }

    return ret;
}

// Struktura: count(varInt), [key_id1(varInt), offset1(varInt)...]
std::vector<IndexEntry> SSTableComp::writeIndexToFile()
{
    uint64_t start_offset = 0;

    if (is_single_file_mode_) {
        start_offset = toc.index_offset; // Set in data writer
    }

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

        uint32_t key_id = key_to_id[ie.key];
        string key_str = varenc::encodeVarint<uint32_t>(key_id);
        string offset_str = varenc::encodeVarint<ull>(ie.offset);

        payload.append(key_str);
        payload.append(offset_str);

        offset += key_str.size() + offset_str.size();
    }

    int block_id = start_offset / block_size;
    size_t total_bytes = payload.size();
    offset = 0;

    while (offset + block_size <= total_bytes) {
        string chunk = payload.substr(offset, block_size);
        bmp->write_block({ block_id++, indexFile_ }, chunk);
        offset += block_size;
    }

    if (offset < total_bytes) {
        std::string chunk = payload.substr(offset);
        bmp->write_block({ block_id++, indexFile_ }, chunk);
    }

    if (is_single_file_mode_) toc.summary_offset = (block_id + 1) * block_size;

    return ret;
}

void SSTableComp::writeSummaryToFile()
{
    uint64_t start_offset = 0;
    if (is_single_file_mode_) {
        start_offset = toc.summary_offset; // Set in index writer
    }

    string payload;

    payload.append(varenc::encodeVarint<uint64_t>(summary_.summary.size())); // Count

    uint32_t min_key_val = key_to_id[summary_.min];
    uint32_t max_key_val = key_to_id[summary_.max];

    string min_key_str = varenc::encodeVarint<uint32_t>(min_key_val);
    string max_key_str = varenc::encodeVarint<uint32_t>(max_key_val);


    payload.append(min_key_str);
    payload.append(max_key_str);


    for (auto& summEntry : summary_.summary) {
        string k_str = varenc::encodeVarint<uint32_t>(key_to_id[summEntry.key]);
        payload.append(k_str);

        string offset_str = varenc::encodeVarint<ull>(summEntry.offset);
        payload.append(offset_str);

    }

    int block_id = start_offset / block_size;
    uint64_t total_bytes = payload.size();
    uint64_t offset = 0;

    while (offset + block_size <= total_bytes) {
        string chunk = payload.substr(offset, block_size);
        bmp->write_block({ block_id++, summaryFile_ }, chunk);
        offset += block_size;
    }

    if (offset < total_bytes) {
        std::string chunk = payload.substr(offset);
        bmp->write_block({ block_id++, summaryFile_ }, chunk);
    }

    if (is_single_file_mode_) toc.filter_offset = (block_id + 1) * block_size;
}

// U SSTableRaw.cpp i SSTableComp.cpp

void SSTableComp::readMetaFromFile() {
    rootHash_.clear();
    originalLeafHashes_.clear();

    uint64_t offset = toc.meta_offset;

    size_t rootHashSize;
    readBytes(&rootHashSize, sizeof(rootHashSize), offset, metaFile_);

    rootHash_.resize(rootHashSize);
    readBytes(&rootHash_[0], rootHashSize, offset, metaFile_);

    size_t leafCount;
    readBytes(&leafCount, sizeof(leafCount), offset, metaFile_);

    originalLeafHashes_.reserve(leafCount);

    for(int i = 0; i < leafCount; i++) {
        size_t leafSize;
        readBytes(&leafSize, sizeof(leafSize), offset, metaFile_);

        string leaf;
        leaf.resize(leafSize);
        leaf.reserve(leafSize);
        readBytes(&leaf[0], leafSize, offset, metaFile_);

        originalLeafHashes_.push_back(leaf);
    }
}

void SSTableComp::writeBloomToFile()
{
    uint64_t start_offset = 0;
    if (is_single_file_mode_) {
        start_offset = toc.filter_offset; // Set in summary writer
    }

    std::vector<byte> raw = bloom_.serialize();

    string len_str = varenc::encodeVarint<size_t>(raw.size());

    string payload;


    payload.reserve(raw.size());

    payload.append(len_str);
    payload.append(reinterpret_cast<const char*>(raw.data()), raw.size());

    int block_id = start_offset / block_size;
    size_t total_bytes = payload.size();
    size_t offset = 0;

    while (offset + block_size <= total_bytes) {
        string chunk = payload.substr(offset, block_size);
        bmp->write_block({ block_id++, filterFile_ }, chunk);
        offset += block_size;
    }

    if (offset < total_bytes) {
        std::string chunk = payload.substr(offset);
        bmp->write_block({ block_id++, filterFile_ }, chunk);
    }

    if (is_single_file_mode_) toc.meta_offset = (block_id + 1) * block_size;
}

// U SSTableRaw.cpp i SSTableComp.cpp

void SSTableComp::writeMetaToFile() { // (ili SSTableComp::writeMetaToFile)
    string payload;

    // 1. Upisujemo dužinu root hasha, pa sam hash
    size_t rootHashSize = rootHash_.size();
    payload.append(reinterpret_cast<const char*>(&rootHashSize), sizeof(rootHashSize));
    payload.append(rootHash_);

    // 2. Upisujemo broj listova (leaf hashes)
    size_t leafCount = originalLeafHashes_.size();
    payload.append(reinterpret_cast<const char*>(&leafCount), sizeof(leafCount));

    // 3. Upisujemo svaki list (njegovu dužinu, pa sam hash)
    for (const auto& leaf : originalLeafHashes_) {
        size_t leafSize = leaf.size();
        payload.append(reinterpret_cast<const char*>(&leafSize), sizeof(leafSize));
        payload.append(leaf);
    }

    // Zapisujemo ceo payload u meta fajl (ova logika je vaša postojeća)
    int block_id = toc.meta_offset / block_size;
    size_t offset = 0;
    while (offset < payload.length()) {
        string chunk = payload.substr(offset, block_size);
        bmp->write_block({ block_id++, metaFile_ }, chunk);
        offset += chunk.size();
    }
}

void SSTableComp::readBloomFromFile()
{
    uint64_t fileOffset = 0;
    uint64_t len = 0;

    if (!readNumValue<uint64_t>(len, fileOffset, filterFile_)) {
        std::cerr << "[SSTableComp::readBloomFromFile] Problem reading bloom length\n";
        return;
    }

    if (len == 0) return;

    std::vector<byte> raw(len);
    if (!readBytes(raw.data(), len, fileOffset, filterFile_)) {
        std::cerr << "[SSTableComp::readBloomFromFile] Problem reading bloomfilter\n";
        return;
    }

    bloom_ = BloomFilter::deserialize(raw);
}

void SSTableComp::readSummaryHeader()
{
    summary_.min.clear();
    summary_.max.clear();

    uint64_t offset = toc.summary_offset;
    if (!readNumValue<uint64_t>(summary_.count, offset, summaryFile_)) {
        std::cerr << "[SSTableComp::readSummaryHeader] Problem reading summary count\n";
        return;
    }

    uint32_t min_key_id = 0;
    if (!readNumValue<uint32_t>(min_key_id, offset, summaryFile_)) {
        std::cerr << "[SSTableComp::readSummaryHeader] Problem reading min_key\n";
        return;
    }

    summary_.min = id_to_key[min_key_id];

    uint32_t max_key_id = 0;
    if (!readNumValue<uint32_t>(max_key_id, offset, summaryFile_)) {
        std::cerr << "[SSTableComp::readSummaryHeader] Problem reading max_key\n";
        return;
    }

    summary_.max = id_to_key[max_key_id];


    summary_data_start = offset;
}

uint64_t SSTableComp::findRecordOffset(const std::string& key, bool& in_file)
{
    prepare();

    in_file = true;
    if (key > summary_.max) {
        in_file = false;
        return std::numeric_limits<uint64_t>::max();
    }

    if (key < summary_.min) {
        in_file = false;
        return toc.data_offset;
    }

    uint64_t file_offset = summary_data_start;

    bool error;

    uint64_t last_offset = 0;

    uint32_t r_key_id;
    string r_key;
    for (int i = 0; i < summary_.count; i++) {
        readNumValue<uint32_t>(r_key_id, file_offset, summaryFile_);

        r_key = id_to_key[r_key_id];


        if (r_key > key) {
            break;
        }

        readNumValue<uint64_t>(last_offset, file_offset, summaryFile_);
    }

    file_offset = last_offset + toc.index_offset;
    uint64_t off = 0;

    while (true) {
        if (is_single_file_mode_ && file_offset + sizeof(r_key_id) >= toc.summary_offset) break;
        if (!readNumValue<uint32_t>(r_key_id, file_offset, indexFile_)) break;

        r_key = id_to_key[r_key_id];

        if (r_key > key) {
            break;
        }

        readNumValue<uint64_t>(off, file_offset, indexFile_);

        if (r_key == key) {
            break;
        }
    }

    uint64_t fileOffset = off;
    // const uint64_t header =
    //     sizeof(uint)
    //   + sizeof(Wal_record_type)
    //   + sizeof(uint64_t)
    //   + sizeof(char)
    //   + sizeof(uint64_t)
    //   + sizeof(uint32_t);

    const uint64_t header_max_len = sizeof(uint) + sizeof(ull) + 1 + 1 + sizeof(uint64_t) + sizeof(uint32_t);

    while (true) {
        if (fileOffset >= toc.data_end) break; // EOF

        uint64_t rem = block_size - (fileOffset % block_size);
        if (rem < header_max_len) {
            fileOffset += rem;
        }

        uint64_t recordStart = fileOffset;

        uint crc = 0;
        readNumValue<uint>(crc, fileOffset, dataFile_);

        Wal_record_type flag;
        readBytes(&flag, sizeof(flag), fileOffset, dataFile_);

        uint64_t ts = 0;
        readNumValue(ts, fileOffset, dataFile_);

        char tomb;
        readBytes(&tomb, sizeof(tomb), fileOffset, dataFile_);
        bool isTomb = static_cast<bool>(tomb);

        uint64_t v_size = 0;
        if (!isTomb) {
            readBytes(&v_size, sizeof(v_size), fileOffset, dataFile_);
        }

        uint32_t r_key_id = 0;
        readNumValue(r_key_id, fileOffset, dataFile_);
        std::string rkey = id_to_key[r_key_id];

        fileOffset += v_size; // skip value

        if (rkey == key) {
            return recordStart;
        }

        if (rkey > key) {
            in_file = false;
            return recordStart;
        }
    }

    in_file = false;
    return std::numeric_limits<uint64_t>::max();
}

Record SSTableComp::getNextRecord(uint64_t& offset, bool& error, bool& eof) {
    prepare();
    
    const uint64_t header_max_len =  sizeof(uint) + sizeof(ull) + 1 + 1 + sizeof(uint64_t) + sizeof(uint32_t);

    if(offset >= toc.data_end || offset < toc.data_offset) {
        error = true;
        Record r;
        return r;
    }

    uint64_t rem = block_size - (offset % block_size);
    if (rem < header_max_len) {
        offset += rem;
    }

    uint crc = 0;
    uint64_t start = offset;
    readNumValue<uint>(crc, offset, dataFile_);
    uint64_t crc_size = offset - start;


    Wal_record_type flag;
    readBytes(&flag, sizeof(flag), offset, dataFile_);

    start = offset;
    uint64_t ts = 0;
    readNumValue(ts, offset, dataFile_);
    uint64_t ts_size = offset - start;

    char tomb;
    readBytes(&tomb, sizeof(tomb), offset, dataFile_);

    bool isTomb = (bool)tomb;

    uint64_t v_size = 0;
    if(!isTomb) readBytes(&v_size, sizeof(v_size), offset, dataFile_);

    start = offset;
    uint32_t r_key_id = 0;
    readNumValue(r_key_id, offset, dataFile_);
    uint64_t r_key_size = offset - start;

    string rkey = id_to_key[r_key_id];

    std::string rvalue;
    rvalue.resize(v_size);
    readBytes(&rvalue[0], v_size, offset, dataFile_);

    Record r;
    r.crc = crc;
    r.timestamp = ts;
    r.tombstone = static_cast<byte>(tomb);
    r.key_size = rkey.size();
    r.value_size = v_size;
    r.key = rkey;
    r.value = rvalue;

    if (flag == Wal_record_type::FIRST) {
        while(true) {
            offset += crc_size;
            
            Wal_record_type flag;
            readBytes(&flag, sizeof(flag), offset, dataFile_);
            
            offset += ts_size;
            
            offset += sizeof(tomb);
            
            uint64_t vSize = 0;
            if(!isTomb) readBytes(&vSize, sizeof(vSize), offset, dataFile_);
            
            std::string rvalue;
            rvalue.resize(vSize);
            readBytes(&rvalue[0], vSize, offset, dataFile_);
            
            r.value.append(rvalue);
            r.value_size += vSize;
            
            if(flag == Wal_record_type::LAST){
                break;
            }
        }
    }

    if(offset==toc.data_end) eof=true;

    return r;
}

bool SSTableComp::validate() {
    // 1. Učitavamo metapodatke (TOC i Merkle stablo)
    prepare();
    readMetaFromFile();

    // 2. Provera da li Merkle stablo uopšte postoji za ovu tabelu
    if (rootHash_.empty()) {
        std::cout << "[Validate] Merkle hash nije pronađen za SSTable: " << dataFile_
            << ". Tabela se smatra validnom (moguće da je starija verzija)." << std::endl;
        return true;
    }

    std::cout << "\n[Validate] Započeta validacija za SSTable: " << dataFile_ << std::endl;
    std::cout << "[Validate] Očekivani (sačuvani) Root Hash: " << rootHash_ << std::endl;

    // 3. Čitamo sve rekorde iz data fajla da bismo rekonstruisali stanje
    std::vector<Record> records;
    uint64_t offset = getDataStartOffset();
    bool error = false, eof = false;

    while (!eof) {
        Record rec = getNextRecord(offset, error, eof);
        if (error) {
            std::cerr << "[Validate] Greška pri čitanju rekorda tokom validacije." << std::endl;
            return false;
        }
        if (!eof) {
            records.push_back(rec);
        }
    }

    // 4. Kreiramo novo Merkle stablo od trenutnih vrednosti u fajlu
    std::vector<std::string> currentValues;
    currentValues.reserve(records.size());
    for (const auto& rec : records) {
        currentValues.push_back(std::string(reinterpret_cast<const char*>(rec.value.data()), rec.value.size()));
    }

    // Ako nema rekorda, a imamo root hash, to je greška
    if (currentValues.empty() && !rootHash_.empty()) {
        std::cout << "[Validate] GREŠKA: Tabela bi trebalo da sadrži podatke, ali je prazna!" << std::endl;
        return false;
    }

    MerkleTree newMerkleTree(currentValues);
    std::string newRootHash = newMerkleTree.getRootHash();
    std::cout << "[Validate] Trenutni (izračunati) Root Hash:  " << newRootHash << std::endl;

    // 5. Poredimo root hasheve
    if (newRootHash == rootHash_) {
        std::cout << "[Validate] USPEH: Podaci u SSTable su validni i nisu menjani." << std::endl;
        return true;
    }

    // 6. Ako hashevi NISU isti, započinjemo detaljnu analizu problema
    std::cout << "[Validate] GREŠKA: Podaci u SSTable su izmenjeni! Detaljna analiza:" << std::endl;
    std::vector<std::string> newLeafHashes = newMerkleTree.getLeaves();

    size_t max_leaves = std::max(originalLeafHashes_.size(), newLeafHashes.size());

    for (size_t i = 0; i < max_leaves; ++i) {
        bool original_exists = i < originalLeafHashes_.size();
        bool new_exists = i < newLeafHashes.size();

        if (original_exists && new_exists) {
            // Slučaj 1: Hash postoji u obe verzije, proveravamo da li je isti
            if (originalLeafHashes_[i] != newLeafHashes[i]) {
                std::cout << "  -> IZMENA na rekordu sa indeksom " << i << "." << std::endl;
                std::cout << "     Ključ rekorda: " << records[i].key << std::endl;
                std::cout << "     Originalni hash vrednosti: " << originalLeafHashes_[i] << std::endl;
                std::cout << "     Trenutni hash vrednosti:   " << newLeafHashes[i] << std::endl;
            }
        }
        else if (original_exists && !new_exists) {
            // Slučaj 2: Hash postoji u originalu, ali ne i u novoj verziji -> Rekord je obrisan
            std::cout << "  -> OBRISAN rekord koji je bio na indeksu " << i << "." << std::endl;
            // Ne možemo znati ključ jer rekord više ne postoji
        }
        else if (!original_exists && new_exists) {
            // Slučaj 3: Hash ne postoji u originalu, ali postoji u novoj verziji -> Rekord je dodat
            std::cout << "  -> DODAT novi rekord na indeksu " << i << "." << std::endl;
            std::cout << "     Ključ rekorda: " << records[i].key << std::endl;
            std::cout << "     Hash vrednosti: " << newLeafHashes[i] << std::endl;
        }
    }

    return false;
}