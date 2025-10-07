#include "SSTableRaw.h"
#include "../Utils/VarEncoding.h"
// #include "../LSM/SSTableIterator.h"
#include <filesystem>

SSTableRaw::SSTableRaw(const std::string& dataFile,
	const std::string& indexFile,
	const std::string& filterFile,
	const std::string& summaryFile,
	const std::string& metaFile,
	Block_manager* bmp)
	: SSTable(dataFile, indexFile, filterFile, summaryFile, metaFile, bmp)
{
}

SSTableRaw::SSTableRaw(const std::string& dataFile,
	Block_manager* bmp)
	: SSTable(dataFile, bmp)
{
}

/*
bool SSTableRaw::validate() {
    std::cout << "[SSTable] Pokrecem validaciju integriteta za: " << dataFile_ << std::endl;

    // --- 1. Ucitaj originalni Root Hash iz meta fajla ---
    readMetaFromFile();

    if (rootHash_.empty()) {
        try {
            // Ako nema meta fajla => ispravno je samo ako nema ni data fajla
            bool file_exists_and_not_empty = std::filesystem::exists(dataFile_) && std::filesystem::file_size(dataFile_) > 0;
            if (file_exists_and_not_empty) {
                std::cerr << "[SSTable] GREŠKA: Data fajl postoji, ali za njega nisu sačuvani Merkle metapodaci." << std::endl;
                return false;
            }
            std::cout << "[SSTable] VALIDACIJA USPEŠNA: Ne postoje ni metapodaci ni data fajl." << std::endl;
            return true;
        }
        catch (...) { return true; } // Ako fajl ne postoji => fs::exists baca izuzetak
    }
    std::cout << "  Originalni (sacuvani) Root Hash: " << rootHash_ << std::endl;

    // --- 2. Ponovo procitaj sve zapise i sakupi podatke ---
    std::vector<std::string> data_for_merkle;
    try {
        SSTableMetadata temp_meta;
        temp_meta.data_path = dataFile_;
        SSTableIterator iter(temp_meta, this->block_size);

        while (iter.hasNext()) {
            Record r = iter.next();
            data_for_merkle.push_back(r.key + r.value);
        }
    }
    catch (const std::exception& e) {
        std::cerr << "Greska prilikom citanja data fajla tokom validacije: " << e.what() << std::endl;
        return false;
    }

    if (data_for_merkle.empty() && !rootHash_.empty()) {
        std::cerr << "[SSTable] GREŠKA: Data fajl je prazan, a metapodaci postoje. Fajl je oštećen." << std::endl;
        return false;
    }

    // --- 3. Izracunaj novi root hash od trenutnih podataka ---
    MerkleTree new_merkle_tree(data_for_merkle);
    std::string new_root_hash = new_merkle_tree.getRootHash();
    std::cout << "  Novokreirani (trenutni) Root Hash: " << new_root_hash << std::endl;

    // --- 4. Uporedi "otiske" ---
    if (new_root_hash == rootHash_) {
        std::cout << "[SSTable] VALIDACIJA USPEŠNA. Integritet podataka je potvrđen." << std::endl;
        return true;
    }
    
	// --- 5. Detaljna analiza => ako se koreni ne poklapaju, gde je greska? ---
    std::cerr << "[SSTable] GREŠKA: VALIDACIJA NEUSPEŠNA! Root hashevi se ne poklapaju." << std::endl;
    std::cerr << "  Originalni Hash: " << rootHash_ << std::endl;
    std::cerr << "  Trenutni Hash:   " << new_root_hash << std::endl;

    // Uporedi listu po listu
    auto new_leaves = new_merkle_tree.getLeaves();
    if (new_leaves.size() != originalLeafHashes_.size()) {
        std::cerr << "  DETEKCIJA: Broj zapisa je promenjen! Originalno: "
            << originalLeafHashes_.size() << ", Trenutno: " << new_leaves.size() << std::endl;
        return false;
    }

    for (size_t i = 0; i < originalLeafHashes_.size(); ++i) {
        if (originalLeafHashes_[i] != new_leaves[i]) {
            std::cerr << "  DETEKCIJA: Prva promena je detektovana na zapisu sa indeksom " << i << "." << std::endl;
            // Za još detaljniju analizu, ovde biste mogli da koristite Merkle Proof
            return false;
        }
    }

    std::cerr << "  DETEKCIJA: Nepoznata greška, listovi se poklapaju ali koreni ne (ovo ne bi smelo da se desi)." << std::endl;
    return false;
}
*/

// Funkcija NE PROVERAVA bloomfilter, pretpostavlja da je vec provereno
vector<Record> SSTableRaw::get(const std::string& key) {
    prepare();
    vector<Record> matches;

    // 3) binarna pretraga -> offset
    bool found;
    uint64_t fileOffset = findRecordOffset(key, found);
    if (!found)
    {
        return matches;
    }

    const uint64_t header_len =  sizeof(uint) + sizeof(ull) + 1 + 1 + sizeof(ull) + sizeof(ull);

    // 4) Otvorimo dataFile, idemo od fileOffset redom
    while (true) {
        if (fileOffset >= toc.data_end || fileOffset < toc.data_offset) break;

        if(block_size - (fileOffset % block_size) < header_len) {
            fileOffset += block_size - (fileOffset % block_size);
        } // Ako u bloku posle recorda nema mesta za header, znaci da smo padovali i sledeci record pocinje u sledecem bloku

        // citamo polja Record-a
        uint32_t crc = 0;
        readBytes(&crc, sizeof(crc), fileOffset, dataFile_);

        Wal_record_type flag;
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

        
        if (flag == Wal_record_type::FIRST) {
            while(true) {
                fileOffset += sizeof(crc);
                
                Wal_record_type flag;
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
                
                if(flag == Wal_record_type::LAST){
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

std::vector<IndexEntry>
SSTableRaw::writeDataMetaFiles(std::vector<Record>& sortedRecords)
{
    toc.data_offset = (sizeof(toc)/block_size + 1)*block_size; // Mesto za toc

    std::vector<IndexEntry> ret;
    ret.reserve(sortedRecords.size());

    // Merkle tree
    std::vector<std::string> data_for_merkle;
    data_for_merkle.reserve(sortedRecords.size());

    ull offset = toc.data_offset;

    int block_id = toc.data_offset/block_size;

    string concat;

    auto append_field = [&](const void* data, size_t len) {
        auto ptr = reinterpret_cast<const char*>(data);
        concat.append(ptr, len);
    };

   size_t header_len = sizeof(uint) + 2*sizeof(byte) + 3*sizeof(ull);

    for (auto& r : sortedRecords) {

        data_for_merkle.push_back(r.key + r.value);

    
        IndexEntry ie;
        ie.key = r.key;
        ie.offset = offset;
        ret.push_back(ie);


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

            // Flush
            if(remaining==len){
                bmp->write_block({block_id++, dataFile_}, concat);
                concat.clear();
            }
        }
    }

    if (!concat.empty()) 
        bmp->write_block({block_id, dataFile_}, concat);

    if(is_single_file_mode_) toc.index_offset = (block_id+1)*block_size;
    toc.data_end = block_id*block_size + concat.size();


    if (!data_for_merkle.empty()) {
        try {
            MerkleTree merkle_tree(data_for_merkle);
            this->rootHash_ = merkle_tree.getRootHash();
            this->originalLeafHashes_ = merkle_tree.getLeaves();
            std::cout << "[SSTable] Merkle Tree created with root: " << this->rootHash_ << std::endl;
        }
        catch (const std::exception& e) {
            std::cerr << "Error creating Merkle Tree: " << e.what() << std::endl;
        }
    }


    return ret;
}

std::vector<IndexEntry> SSTableRaw::writeIndexToFile()
{
    uint64_t start_offset = toc.index_offset; // Set in data writer


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

        offset += sizeof(kSize) + kSize + sizeof(ie.offset);
    }

    int block_id = start_offset/block_size;
    size_t total_bytes = payload.size();
    offset = 0;

    while (offset + block_size <= total_bytes) {
        string chunk = payload.substr(offset, block_size);
        bmp->write_block({block_id++, indexFile_}, chunk);

        const uint64_t* val = reinterpret_cast<const uint64_t*>(chunk.data());
        std::cout << *val << "\n";
        
        offset += block_size;
    }

    if (offset < total_bytes) {
        std::string chunk = payload.substr(offset);
        bmp->write_block({ block_id++, indexFile_ }, chunk);
    }

    if(is_single_file_mode_) toc.summary_offset = (block_id+1)*block_size;

    return ret;
}

void SSTableRaw::writeSummaryToFile() {
    uint64_t start_offset = toc.summary_offset; // Set in index writer

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

    int block_id = start_offset/block_size;
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

    if(is_single_file_mode_) toc.filter_offset = (block_id+1)*block_size;
}

// U SSTableRaw.cpp i SSTableComp.cpp

void SSTableRaw::readMetaFromFile() {
    rootHash_.clear();
    originalLeafHashes_.clear();
    prepare();

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
        readBytes(&leaf[0], leafSize, offset, metaFile_);

        originalLeafHashes_.push_back(leaf);
    }
}

void SSTableRaw::writeBloomToFile()
{
    uint64_t start_offset = toc.filter_offset; // Set in summary writer


    std::vector<byte> raw = bloom_.serialize();

    uint64_t len = raw.size();

    string payload;

    payload.append(reinterpret_cast<const char*>(&len), sizeof(len));

    for (const auto& i : raw) {
        payload.append(reinterpret_cast<const char*>(&i), sizeof(i));
    }

    int block_id = start_offset/block_size;
    size_t total_bytes = payload.size();
    size_t offset = 0;

    while (offset + block_size <= total_bytes) {
        string chunk = payload.substr(offset, block_size);
        bmp->write_block({block_id++, filterFile_}, chunk);
        offset += block_size;
    }

    if (offset < total_bytes) {
        std::string chunk = payload.substr(offset);
        bmp->write_block({ block_id++, filterFile_ }, chunk);
    }

    if(is_single_file_mode_) toc.meta_offset = (block_id+1)*block_size;
}

void SSTableRaw::writeMetaToFile() { // (ili SSTableComp::writeMetaToFile)
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
    std::cout << "      Upisujem Payload za : " << metaFile_ << " : ";
    for (char c : payload) {
        std::cout << hex << c;
    }
    std::cout << endl;
    while (offset < payload.length()) {
        string chunk = payload.substr(offset, block_size);
        bmp->write_block({ block_id++, metaFile_ }, chunk);
        offset += chunk.size();
    }
}


void SSTableRaw::readBloomFromFile()
{
    uint64_t fileOffset = 0;

    fileOffset = toc.filter_offset;

    uint64_t len = 0;
    if (!readBytes(&len, sizeof(len), fileOffset, filterFile_) || len == 0) {
        return;
    }

    std::vector<byte> raw(len);
    if (!readBytes(raw.data(), len, fileOffset, filterFile_)) {
        return;
    }

    bloom_ = BloomFilter::deserialize(raw);
}

void SSTableRaw::readSummaryHeader()
{
    summary_.min.clear();
    summary_.max.clear();

    
    uint64_t offset = toc.summary_offset;

    
    uint64_t min_len;
    uint64_t max_len;
    uint64_t count;

    if (!readBytes(&min_len, sizeof(min_len), offset, summaryFile_)) {
        std::cerr << "[SSTableRaw::readSummaryHeader] Problem reading summary min_key_len\n";
        return;
    }
    
    if (!readBytes(&max_len, sizeof(max_len), offset, summaryFile_)) {
        std::cerr << "[SSTableComp::readSummaryHeader] Problem reading max_key_len\n";
        return;
    }

    if (!readBytes(&count, sizeof(count), offset, summaryFile_)) {
        std::cerr << "[SSTableComp::readSummaryHeader] Problem reading count\n";
        return;
    }

    summary_.min.resize(min_len);
    summary_.max.resize(max_len);

    if (!readBytes(&summary_.min[0], min_len, offset, summaryFile_)) {
        std::cerr << "[SSTableComp::readSummaryHeader] Problem reading min key\n";
        return;
    }

    if (!readBytes(&summary_.max[0], max_len, offset, summaryFile_)) {
        std::cerr << "[SSTableComp::readSummaryHeader] Problem reading max key\n";
        return;
    }

    summary_.count = count;
}

uint64_t SSTableRaw::findRecordOffset(const std::string& key, bool& found)
{
    prepare();

    if (key > summary_.max) {
        found = false;
        return std::numeric_limits<uint64_t>::max();
    }

    if (key < summary_.min) {
        found = false;
        return toc.data_offset;
    }

    size_t fileOffset = toc.summary_offset + summary_.min.size() + summary_.max.size() + 3*sizeof(uint64_t);
    
    uint64_t kSize;
    bool error;

    uint64_t offset_in_index = 0ULL;
    
    string rKey;
    for(int i = 0; i < summary_.count; i++) {
        readBytes(&kSize, sizeof(kSize), fileOffset, summaryFile_);

        rKey.resize(kSize);
        readBytes(&rKey[0], kSize, fileOffset, summaryFile_);

        if(rKey > key) {
            break;
        }

        readBytes(&offset_in_index, sizeof(offset_in_index), fileOffset, summaryFile_);
    }

    fileOffset = offset_in_index + toc.index_offset;
    uint64_t offset_in_data;


    for(;;) {
        readBytes(&kSize, sizeof(kSize), fileOffset, indexFile_);
        
        rKey.resize(kSize);
        readBytes(&rKey[0], kSize, fileOffset, indexFile_);

        if (rKey > key) {
            break;
        }

        readBytes(&offset_in_data, sizeof(offset_in_data), fileOffset, indexFile_);

        if (rKey == key) {
            break;
        }
    }

    fileOffset = offset_in_data;

    const uint64_t header_len =  sizeof(uint) + sizeof(ull) + 1 + 1 + sizeof(ull) + sizeof(ull);


    while (true) {
        if(block_size - (fileOffset % block_size) < header_len) {
            fileOffset += block_size - (fileOffset % block_size);
        } // Ako u bloku posle recorda nema mesta za header, znaci da smo padovali i sledeci record pocinje u sledecem bloku

        uint64_t saved_offset = fileOffset;

        // citamo polja Record-a
        uint32_t crc = 0;
        fileOffset += sizeof(crc);

        Wal_record_type flag;
        readBytes(&flag, sizeof(flag), fileOffset, dataFile_);

        uint64_t ts = 0;
        fileOffset += sizeof(ts);

        char tomb = '0';
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

        Record r;
        r.crc = crc;
        r.timestamp = ts;
        r.tombstone = static_cast<byte>(tomb);
        r.key_size = kSize;
        r.value_size = vSize;
        r.key = rkey;
        r.value = rvalue;

        
        if (flag == Wal_record_type::FIRST) {
            while(true) {
                fileOffset += sizeof(crc);
                
                Wal_record_type flag;
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
                
                if(flag == Wal_record_type::LAST){
                    break;
                }
            }
        }
        
        if(r.key == key) {
            found = true;
            return saved_offset;
        }
        
        if (r.key > key) {
            // nema smisla ici dalje, data fajl je sortiran
            found = false;
            return saved_offset;
        }
    }

    // Funkcija ne bi trebala doci ovde
    found = false;
    return std::numeric_limits<uint64_t>::max();
}

Record SSTableRaw::getNextRecord(uint64_t& offset, bool& error, bool& eof) {

    const uint64_t header_len =  sizeof(uint) + sizeof(ull) + 1 + 1 + sizeof(ull) + sizeof(ull);
    // TODO: OVAJ DEO TREBA POPRAVITI, ovaj drugi. Za sada ga ignorisem
    if (offset >= toc.data_end /* || offset < toc.data_offset*/) {
        error = true;
        Record r;
        return r;
    };

    if(block_size - (offset % block_size) < header_len) {
        offset += block_size - (offset % block_size);
    } // Ako u bloku posle recorda nema mesta za header, znaci da smo padovali i sledeci record pocinje u sledecem bloku

    // citamo polja Record-a
    uint32_t crc = 0;
    readBytes(&crc, sizeof(crc), offset, dataFile_);

    Wal_record_type flag;
    readBytes(&flag, sizeof(flag), offset, dataFile_);

    uint64_t ts = 0;
    readBytes(&ts, sizeof(ts), offset, dataFile_);

    char tomb;
    readBytes(&tomb, sizeof(tomb), offset, dataFile_);

    uint64_t kSize = 0;
    readBytes(&kSize, sizeof(kSize), offset, dataFile_);

    uint64_t vSize = 0;
    readBytes(&vSize, sizeof(vSize), offset, dataFile_);

    std::string rkey;
    rkey.resize(kSize);
    readBytes(&rkey[0], kSize, offset, dataFile_);

    std::string rvalue;
    rvalue.resize(vSize);
    readBytes(&rvalue[0], vSize, offset, dataFile_);

    Record r;
    r.crc = crc;
    r.timestamp = ts;
    r.tombstone = static_cast<byte>(tomb);
    r.key_size = kSize;
    r.value_size = vSize;
    r.key = rkey;
    r.value = rvalue;

    
    if (flag == Wal_record_type::FIRST) {
        while(true) {
            offset += sizeof(crc);
            
            Wal_record_type flag;
            readBytes(&flag, sizeof(flag), offset, dataFile_);
            
            offset += sizeof(ts);
            
            offset += sizeof(tomb);
            
            uint64_t kSize = 0;
            readBytes(&kSize, sizeof(kSize), offset, dataFile_);
            
            uint64_t vSize = 0;
            readBytes(&vSize, sizeof(vSize), offset, dataFile_);
            
            std::string rkey;
            rkey.resize(kSize);
            readBytes(&rkey[0], kSize, offset, dataFile_);
            
            std::string rvalue;
            rvalue.resize(vSize);
            readBytes(&rvalue[0], vSize, offset, dataFile_);
            
            r.value.append(rvalue);
            r.key.append(rkey);
            r.key_size += kSize;
            r.value_size += vSize;
            
            if(flag == Wal_record_type::LAST){
                break;
            }
        }
    }

    if( offset == toc.data_end ) eof = true;
    
    return r;
}

bool SSTableRaw::validate() {
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
        records.push_back(rec);
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