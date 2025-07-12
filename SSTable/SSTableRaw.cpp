#include "SSTableRaw.h"
#include "../Utils/VarEncoding.h"
#include "../LSM/SSTableIterator.h"

SSTableRaw::SSTableRaw(const std::string& dataFile,
                       const std::string& indexFile,
                       const std::string& filterFile,
                       const std::string& summaryFile,
                       const std::string& metaFile,
                       Block_manager* bmp)
  : SSTable(dataFile, indexFile, filterFile, summaryFile, metaFile, bmp)
{
}

bool SSTableRaw::validate() {
    std::cout << "[SSTable] Pokrecem validaciju integriteta za: " << dataFile_ << std::endl;

    // --- 1. Ucitaj originalni Root Hash iz meta fajla ---
    readMetaFromFile();

    if (rootHash_.empty()) {
        std::cerr << "[SSTable] VALIDACIJA NEUSPEŠNA: Originalni Merkle root hash nije pronađen." << std::endl;
        return false;
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


void SSTableRaw::build(std::vector<Record>& records)
{
    if (records.empty()) {
        std::cerr << "[SSTable] build: Nema zapisa.\n";
        return;
    }

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


std::vector<IndexEntry> SSTableRaw::writeDataMetaFiles(std::vector<Record>& sortedRecords)
{
    std::vector<IndexEntry> index_entries;
    index_entries.reserve(sortedRecords.size());

    // pomocni vektor u kom skupljamo podatke za kasnije Merkle Tree
    std::vector<std::string> data_for_merkle;
    data_for_merkle.reserve(sortedRecords.size());

    uint64_t logical_offset = 0;
    int block_id = 0;

    std::vector<byte> block_buffer;
    block_buffer.reserve(block_size);

    // Pomocna lambda => dodavanje u bafer
    auto append_to_buffer = [&](const void* data, size_t len) {
        const byte* ptr = reinterpret_cast<const byte*>(data);
        block_buffer.insert(block_buffer.end(), ptr, ptr + len);
        };

    for (const auto& record : sortedRecords) {
        // Pripremi Index i Merkle podatke 
        index_entries.push_back({ record.key, logical_offset });
        data_for_merkle.push_back(record.key + record.value);

		// pisanje blokova
        size_t header_len = sizeof(record.crc) + sizeof(Wal_record_type) + sizeof(record.timestamp) +
            sizeof(record.tombstone) + sizeof(record.key_size) + sizeof(record.value_size);
        size_t total_record_len = header_len + record.key_size + record.value_size;

        size_t space_in_current_block = block_size - block_buffer.size();

        if (space_in_current_block >= total_record_len) {
            // --- SLUCAJ 1: Ceo zapis staje u trenutni blok. ---
            Wal_record_type flag = Wal_record_type::FULL;

            append_to_buffer(&record.crc, sizeof(record.crc));
            append_to_buffer(&flag, sizeof(flag));
            append_to_buffer(&record.timestamp, sizeof(record.timestamp));
            append_to_buffer(&record.tombstone, sizeof(record.tombstone));
            append_to_buffer(&record.key_size, sizeof(record.key_size));
            append_to_buffer(&record.value_size, sizeof(record.value_size));
            append_to_buffer(record.key.data(), record.key_size);
            append_to_buffer(record.value.data(), record.value_size);

        }
        else {
            // --- SLUCAJ 2: Zapis mora da se deli preko vise blokova. ---

            size_t key_bytes_processed = 0;
            size_t value_bytes_processed = 0;
            bool is_first_chunk = true;

            while (key_bytes_processed < record.key_size || value_bytes_processed < record.value_size) {
                // Ako u baferu ima necega, znaci da ga moramo prvo upisati na disk
                if (!block_buffer.empty()) {
                    bmp->write_block({ block_id++, dataFile_ }, block_buffer);
                    block_buffer.clear();
                }

                size_t available_for_payload = block_size - header_len;

                size_t key_to_write = std::min((size_t)(record.key_size - key_bytes_processed), available_for_payload);
                size_t value_to_write = std::min((size_t)(record.value_size - value_bytes_processed), available_for_payload - key_to_write);

                bool is_last_chunk = (key_bytes_processed + key_to_write == record.key_size) &&
                    (value_bytes_processed + value_to_write == record.value_size);

                Wal_record_type flag;
                if (is_first_chunk) {
                    flag = Wal_record_type::FIRST;
                    is_first_chunk = false;
                }
                else if (is_last_chunk) {
                    flag = Wal_record_type::LAST;
                }
                else {
                    flag = Wal_record_type::MIDDLE;
                }

                // Upisivanje zaglavlja i dela podataka u bafer
                append_to_buffer(&record.crc, sizeof(record.crc));
                append_to_buffer(&flag, sizeof(flag));
                append_to_buffer(&record.timestamp, sizeof(record.timestamp));
                append_to_buffer(&record.tombstone, sizeof(record.tombstone));
                append_to_buffer(&key_to_write, sizeof(key_to_write));
                append_to_buffer(&value_to_write, sizeof(value_to_write));

                if (key_to_write > 0) {
                    append_to_buffer(record.key.data() + key_bytes_processed, key_to_write);
                }
                if (value_to_write > 0) {
                    append_to_buffer(record.value.data() + value_bytes_processed, value_to_write);
                }

                key_bytes_processed += key_to_write;
                value_bytes_processed += value_to_write;
            }
        }
        logical_offset += total_record_len;
    }

    // upisi poslednji, nepotpuni blok ako postoji
    if (!block_buffer.empty()) {
        bmp->write_block({ block_id, dataFile_ }, block_buffer);
    }

    // kreiraj Merkle stablo i sacuvaj root hash
    if (!data_for_merkle.empty()) {
        try {
            MerkleTree merkle_tree(data_for_merkle);
            this->rootHash_ = merkle_tree.getRootHash();
			this->originalLeafHashes_ = merkle_tree.getLeaves();
            std::cout << "[SSTable] Merkle Tree created. Root: " << this->rootHash_
                << ", Leaves: " << this->originalLeafHashes_.size() << std::endl;
        }
        catch (const std::exception& e) {
            std::cerr << "Error creating Merkle Tree: " << e.what() << std::endl;
        }
    }

    return index_entries;
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
    // Resetujemo stanje pre citanja
    rootHash_.clear();
    originalLeafHashes_.clear();

    bool error = false;
    int block_id = 0;
    std::string content;

    // citamo sve blokove iz meta fajla dok ne dodjemo do kraja
    while (true) {
        std::vector<byte> block_data = bmp->read_block({ block_id++, metaFile_ }, error);
        if (error) { // Ako read_block vrati gresku (npr. nema vise blokova), prekidamo
            break;
        }
        content.append(reinterpret_cast<const char*>(block_data.data()), block_data.size());
    }

    // Ukloni padding karaktere sa kraja celokupnog sadrzaja
    size_t end_pos = content.find(static_cast<char>(padding_character));
    if (end_pos != std::string::npos) {
        content.erase(end_pos);
    }

    if (content.empty()) {
        return; // Fajl je prazan, nema sta da se parsira
    }

    // Parsiramo procitani sadrzaj liniju po liniju
    std::stringstream ss(content);
    std::string line;

    // Prva linija je uvek root hash
    if (std::getline(ss, line) && !line.empty()) {
        rootHash_ = line;
    }

    // Sve ostale linije su hesevi listova
    while (std::getline(ss, line)) {
        if (!line.empty()) {
            originalLeafHashes_.push_back(line);
        }
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
        bmp->write_block({block_id++, filterFile_}, chunk);
        offset += block_size;
    }
}

// NOVI FORMAT U Meta fajlu:
// Prva linija: [root_hash]
// Svaka sledeća linija: [leaf_hash_1], [leaf_hash_2], ..., [leaf_hash_n]
void SSTableRaw::writeMetaToFile() const {
    if (rootHash_.empty()) {
        return; // nema sta da se pise
    }

    // Kreiramo ceo payload sa prelascima u novi red radi lakseg parsiranja kasnije
    std::string payload = rootHash_ + "\n";
    for (const auto& leaf : originalLeafHashes_) {
        payload += leaf + "\n";
    }

	// upisujemo payload u blokove
    int block_id = 0;
    size_t offset = 0;
    while (offset < payload.length()) {
        size_t chunk_size = std::min((size_t)block_size, payload.length() - offset);
        std::string chunk_str = payload.substr(offset, chunk_size);

        std::vector<byte> data_chunk;
        std::transform(chunk_str.begin(), chunk_str.end(), std::back_inserter(data_chunk),
            [](char c) { return std::byte(c); });

        // Pozivamo Block Manager za svaki blok
        bmp->write_block({ block_id++, metaFile_ }, data_chunk);
        offset += chunk_size;
    }
    std::cout << "[SSTable] Merkle root and " << originalLeafHashes_.size()
        << " leaves written to " << metaFile_ << std::endl;
}

void SSTableRaw::readBloomFromFile()
{
    uint64_t fileOffset = 0;

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

