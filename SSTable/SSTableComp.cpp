#include "SSTableComp.h"
#include <filesystem>
#include "../Utils/VarEncoding.h"

SSTableComp::SSTableComp(const std::string& dataFile,
                         const std::string& indexFile,
                         const std::string& filterFile,
                         const std::string& summaryFile,
                         const std::string& metaFile,
                         Block_manager* bmp,
                         unordered_map<string, uint32_t>& map,
                         vector<string>& id_to_key,
                         uint32_t& nextId
                        )
  : SSTable(dataFile, indexFile, filterFile, summaryFile, metaFile, bmp), key_to_id(map), id_to_key(id_to_key), nextID(nextId)
{
}

bool SSTableComp::validate() {
    std::cout << "[SSTableComp] Pokrecem validaciju integriteta za: " << dataFile_ << std::endl;

    // ucitaj rootHash_
    readMetaFromFile();
    if (rootHash_.empty()) {
        std::cerr << "[SSTableComp] GREŠKA: Originalni Merkle root hash nije pronađen." << std::endl;
        return false;
    }
    std::cout << "  Originalni (sacuvani) Root Hash: " << rootHash_ << std::endl;

    // procitaj sve zapise iz kompesovanog data fajla
    std::vector<std::string> data_for_merkle;
    uint64_t current_offset = 0;

    while (true) {
        // Pomoćne promenljive za rekonstrukciju jednog zapisa
        std::string reconstructed_key;
        std::string reconstructed_value;
        Wal_record_type flag;

        // proveravamo da li mozemo da procitamo barem fleg sledećeg zapisa
        // ovo je nas uslov za izlazak iz petlje, umesto poredjenja sa file_size.
        uint64_t peek_offset = current_offset;
        if (!readBytes(&flag, sizeof(flag), peek_offset, dataFile_)) {
            break; // Nema vise bajtova za citanje => kraj fajla
        }

        // Petlja za rekonstrukciju jednog kompletnog zapisa (moze imati vise delova)
        do {
            Record chunk_header; // cuvamo header trenutnog dela

            // citamo header svakog dela (chunk-a)
            if (!readBytes(&flag, sizeof(flag), current_offset, dataFile_)) goto end_of_validation_loop;
            if (!readBytes(&chunk_header.tombstone, sizeof(chunk_header.tombstone), current_offset, dataFile_)) goto end_of_validation_loop;
            if (!readNumValue(chunk_header.crc, current_offset, dataFile_)) goto end_of_validation_loop;
            if (!readNumValue(chunk_header.timestamp, current_offset, dataFile_)) goto end_of_validation_loop;

            // Kljuc ID se cita samo iz prvog dela (FIRST ili FULL)
            if (flag == Wal_record_type::FIRST || flag == Wal_record_type::FULL) {
                uint32_t key_id = 0;
                if (!readNumValue(key_id, current_offset, dataFile_)) goto end_of_validation_loop;
                if (key_id >= id_to_key.size()) {
                    std::cerr << "GREŠKA: Pronadjen nevalidan key_id u data fajlu." << std::endl;
                    return false;
                }
                reconstructed_key = id_to_key[key_id];
            }

            // Vrednost se cita iz svakog dela (ako zapis nije tombstone)
            if (static_cast<int>(chunk_header.tombstone) != 1) {
                uint64_t value_chunk_size = 0;
                if (!readNumValue(value_chunk_size, current_offset, dataFile_)) goto end_of_validation_loop;

                if (value_chunk_size > 0) {
                    std::string value_part(value_chunk_size, '\0');
                    if (!readBytes(&value_part[0], value_chunk_size, current_offset, dataFile_)) goto end_of_validation_loop;
                    reconstructed_value.append(value_part);
                }
            }
        } while (flag == Wal_record_type::FIRST || flag == Wal_record_type::MIDDLE);

        // Sakupi podatke za Merkle stablo nakon sto je ceo zapis rekonstruisan
        data_for_merkle.push_back(reconstructed_key + reconstructed_value);
    }

end_of_validation_loop:; // Labela za izlazak iz ugnezdenih petlji

    // izracunaj novi hahs i uporedi
    MerkleTree new_merkle_tree(data_for_merkle);
    std::string new_root_hash = new_merkle_tree.getRootHash();

    std::cout << "  Originalni (sacuvani) Root Hash: " << rootHash_ << std::endl;
    std::cout << "  Novokreirani (trenutni) Root Hash: " << new_root_hash << std::endl;

    if (new_root_hash == rootHash_) {
        std::cout << "[SSTableComp] VALIDACIJA USPEŠNA. Integritet podataka je potvrđen." << std::endl;
        return true;
    }
    else {
        std::cerr << "[SSTableComp] GREŠKA: VALIDACIJA NEUSPEŠNA! Podaci su menjani ili oštećeni." << std::endl;
        
        return false;
    }
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

    // Pravimo summary
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

    cout << summary_.min << " " << summary_.max << endl;

    // 3) pretraga -> offset
    bool found;
    uint64_t fileOffset = findDataOffset(key, found);
    if (!found)
    {
        return matches;
    }

    size_t header_len = 0;

    // 4) Otvorimo dataFile, idemo od fileOffset redom
    while (true) {
        if(block_size - (fileOffset % block_size) <
            sizeof(uint) + sizeof(ull) + 1 + 1 + sizeof(uint64_t) + sizeof(uint32_t)) {
            fileOffset += block_size - (fileOffset % block_size);
        } // Ako nema mesta za header u najgorem slucaju, paddujemo i to treba da se preskoci.
          // Lose resenje, trebalo bi da se cita index i preskoci na sledeci offset, ali ubicu se ako jos to treba da napisem

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
        if(!isTomb) readBytes(&v_size, sizeof(v_size), fileOffset, dataFile_);

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

        if(rkey == key) {
            if (flag == Wal_record_type::FIRST) {
                while(true) {
                    fileOffset += crc_size;
                    
                    Wal_record_type flag;
                    readBytes(&flag, sizeof(flag), fileOffset, dataFile_);
                    
                    fileOffset += ts_size;
                    
                    fileOffset += sizeof(tomb);
                    
                    uint64_t vSize = 0;
                    if(!isTomb) readBytes(&vSize, sizeof(vSize), fileOffset, dataFile_);
                    
                    std::string rvalue;
                    rvalue.resize(vSize);
                    readBytes(&rvalue[0], vSize, fileOffset, dataFile_);
                    
                    r.value.append(rvalue);
                    r.value_size += vSize;
                    
                    if(flag == Wal_record_type::LAST){
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

// Struktura: [crc(varInt), flag(byte), timestamp(varInt), tombstone(byte), val_size(uint64), key_id(varInt), value(string)]
// Kada se splituje, samo prvi ima key_id
std::vector<IndexEntry> SSTableComp::writeDataMetaFiles(std::vector<Record>& sortedRecords)
{
    std::vector<IndexEntry> index_entries;
    index_entries.reserve(sortedRecords.size());

    std::vector<std::string> data_for_merkle;
    data_for_merkle.reserve(sortedRecords.size());

    uint64_t logical_offset = 0;
    int block_id = 0;
    std::string block_buffer;
    block_buffer.reserve(block_size);

    for (const auto& record : sortedRecords) {
        // --- KORAK 1: Pripremi Index i Merkle podatke ---
        index_entries.push_back({ record.key, logical_offset });
        data_for_merkle.push_back(record.key + record.value);

        // --- KORAK 2: Pripremi enkodirane delove headera ---
        bool is_tombstone = static_cast<bool>(record.tombstone);
        std::string crc_varint = varenc::encodeVarint(record.crc);
        std::string ts_varint = varenc::encodeVarint(record.timestamp);

        uint32_t key_id;
        auto it = key_to_id.find(record.key);

        if (it == key_to_id.end()) {
            // Ključ ne postoji u rečniku, dodajemo ga.
            key_id = nextID++;
            key_to_id[record.key] = key_id;
            id_to_key.push_back(record.key); // Održavamo i inverznu mapu za čitanje
        }
        else {
            // Ključ već postoji, samo pročitamo njegov ID.
            key_id = it->second;
        }
        std::string key_id_varint = varenc::encodeVarint(key_id);

        // --- KORAK 3: Serijalizuj CEO zapis u jedan bafer u memoriji ---
        std::string serialized_record;
        // Format: [flag(1B)][tomb(1B)][crc...][ts...][key_id...][(ako nije tomb)val_size(8B)][(ako nije tomb)value...]
        // Za kompresovanu tabelu, `key_size` i `value_size` se menjaju po delovima.

        // --- KORAK 4: Podeli serijalizovani zapis na delove (chunks) ---
        std::vector<std::string> chunks;
        const std::string& value_to_write = is_tombstone ? "" : record.value;
        size_t value_offset = 0;
        bool first_chunk = true;

        // Petlja koja deli zapis na delove ako je potrebno
        while (value_offset < value_to_write.length() || first_chunk) {
            std::string current_chunk;
            size_t header_size_approx = 1 + 1 + crc_varint.length() + ts_varint.length() + (first_chunk ? key_id_varint.length() : 0) + (is_tombstone ? 0 : sizeof(uint64_t));
            size_t max_payload_size = (block_size > header_size_approx) ? block_size - header_size_approx : 0;
            size_t value_chunk_size = std::min(value_to_write.length() - value_offset, max_payload_size);

            Wal_record_type flag;
            if (first_chunk && (value_offset + value_chunk_size == value_to_write.length())) {
                flag = Wal_record_type::FULL;
            }
            else if (first_chunk) {
                flag = Wal_record_type::FIRST;
            }
            else if (value_offset + value_chunk_size == value_to_write.length()) {
                flag = Wal_record_type::LAST;
            }
            else {
                flag = Wal_record_type::MIDDLE;
            }

            // Sastavljanje jednog dela (chunk-a)
            current_chunk.append(reinterpret_cast<const char*>(&flag), sizeof(flag));
            current_chunk.append(reinterpret_cast<const char*>(&record.tombstone), sizeof(record.tombstone));
            current_chunk.append(crc_varint);
            current_chunk.append(ts_varint);
            if (flag == Wal_record_type::FIRST || flag == Wal_record_type::FULL) {
                current_chunk.append(key_id_varint); // key_id ide samo u prvi chunk
            }
            if (!is_tombstone) {
                uint64_t v_size_chunk = value_chunk_size;
                current_chunk.append(reinterpret_cast<const char*>(&v_size_chunk), sizeof(v_size_chunk));
                current_chunk.append(value_to_write, value_offset, value_chunk_size);
            }

            chunks.push_back(current_chunk);
            value_offset += value_chunk_size;
            first_chunk = false;
        }

        // --- KORAK 5: Upisi pripremljene delove u blokove ---
        size_t record_len_on_disk = 0;
        for (const auto& chunk : chunks) {
            if (block_buffer.length() + chunk.length() > block_size) {
                block_buffer.resize(block_size, static_cast<char>(padding_character));
                bmp->write_block({ block_id++, dataFile_ }, block_buffer);
                block_buffer.clear();
            }
            block_buffer.append(chunk);
            record_len_on_disk += chunk.length();
        }
        logical_offset += record_len_on_disk;
    }

    // Upisi poslednji, nepotpuni blok ako postoji
    if (!block_buffer.empty()) {
        block_buffer.resize(block_size, static_cast<char>(padding_character));
        bmp->write_block({ block_id, dataFile_ }, block_buffer);
    }

    // Kreiraj Merkle stablo i sacuvaj root hash
    if (!data_for_merkle.empty()) {
        MerkleTree merkle_tree(data_for_merkle);
        this->rootHash_ = merkle_tree.getRootHash();
        this->originalLeafHashes_ = merkle_tree.getLeaves();
    }

    return index_entries;
}

// Struktura: count(varInt), [key_id1(varInt), offset1(varInt)...]
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

        uint32_t key_id = key_to_id[ie.key];
        string key_str = varenc::encodeVarint<uint32_t>(key_id);
        string offset_str = varenc::encodeVarint<ull>(ie.offset);

        payload.append(key_str);
        payload.append(offset_str);

        offset += key_str.size() + offset_str.size();
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

// Struktura: count(varInt), min_key(varInt), max_key(varInt), [key_id1(varInt), offset1(varInt)...]
void SSTableComp::writeSummaryToFile()
{
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
    // Resetujemo stanje pre citanja
    rootHash_.clear();
    originalLeafHashes_.clear();

    bool error = false;
    int block_id = 0;
    std::string content;

	// procitamo sve blokove iz meta fajla dok ne dodjemo do kraja (blok po blok)
    while (true) {
        std::vector<byte> block_data = bmp->read_block({ block_id++, metaFile_ }, error);
        if (error) {
            break; // nema vise blokova ili je doslo do greske
        }
        // dodajemo sadrzaj procitanog bloka u jedan veliki string
        content.append(reinterpret_cast<const char*>(block_data.data()), block_data.size());
    }

    if (content.empty()) {
        return; // Fajl je prazan, nema sta da se parsira
    }

	// nakon sto smo procitali sve blokove, uklonimo padding karaktere sa kraja celokupnog sadrzaja
    size_t end_pos = content.find(static_cast<char>(padding_character));
    if (end_pos != std::string::npos) {
        content.erase(end_pos);
    }

	// parisamo procitani sadrzaj liniju po liniju
    std::stringstream ss(content);
    std::string line;

    // Prva linija je uvek root hash
    if (std::getline(ss, line)) {
        // Ukloni eventualni '\r' karakter za Windows kompatibilnost
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (!line.empty()) {
            rootHash_ = line;
        }
    }

    // Sve ostale linije su hesevi listova
    while (std::getline(ss, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (!line.empty()) {
            originalLeafHashes_.push_back(line);
        }
    }
}

void SSTableComp::writeBloomToFile() const
{
    std::vector<byte> raw = bloom_.serialize();

    string len_str = varenc::encodeVarint<size_t>(raw.size());

    string payload;

    
    payload.reserve(raw.size());
    
    // payload.append(len_str);

    payload.append(reinterpret_cast<const char*>(raw.data()), raw.size());

    int block_id = 0;
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
}

void SSTableComp::writeMetaToFile() const
{
    if (rootHash_.empty() || originalLeafHashes_.empty()) {
        std::cout << "[SSTable] No Merkle root hash to write for " << metaFile_ << std::endl;
        return;
    }

    // koristimo '\n' kao separator radi lakseg parsiranja pri citanju
    std::string payload = rootHash_ + "\n";
    for (const auto& leaf : originalLeafHashes_) {
        payload += leaf + "\n";
    }
    
	// upisujemo ceo payload u blokove
    int block_id = 0;
    size_t offset = 0;
    while (offset < payload.length()) {
        // Odredi velicinu sledeceg dela za upis
        size_t chunk_size = std::min((size_t)block_size, payload.length() - offset);

        // Uzmi deo payload-a
        std::string chunk_str = payload.substr(offset, chunk_size);
        
		// Pretvori string u vektor bajtova
        std::vector<byte> data_chunk;
        data_chunk.reserve(chunk_str.length());
        std::transform(chunk_str.begin(), chunk_str.end(), std::back_inserter(data_chunk),
            [](char c) { return std::byte(c); });

        bmp->write_block({ block_id++, metaFile_ }, data_chunk);

        // Pomeri ofset na sledeci deo za upis
        offset += chunk_size;
    }

    std::cout << "[SSTable] Merkle root and " << originalLeafHashes_.size()
        << " leaves successfully written to " << metaFile_ << " via Block Manager." << std::endl;
}

void SSTableComp::readBloomFromFile()
{
    uint64_t fileOffset = 0;
    uint64_t len = 0;
    
    if (!readNumValue<uint64_t>(len, fileOffset, filterFile_)) {
        std::cerr << "[SSTableComp::readBloomFromFile] Problem reading bloom length\n";
        return;
    }

    if(len==0) return;

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

    uint64_t offset = 0;
    
    if (!readNumValue<uint64_t>(summary_.count, offset, summaryFile_)) {
        std::cerr << "[SSTableComp::readSummaryHeader] Problem reading summary count\n";
        return;
    }
    
    uint32_t min_key_id = 0;
    if (!readNumValue<uint32_t>(min_key_id, offset, summaryFile_)) {
        std::cerr << "[SSTableComp::readSummaryHeader] Problem reading min_key_len\n";
        return;
    }

    summary_.min = id_to_key[min_key_id];
    
    uint32_t max_key_id = 0;
    if (!readNumValue<uint32_t>(max_key_id, offset, summaryFile_)) {
        std::cerr << "[SSTableComp::readSummaryHeader] Problem reading max_key_len\n";
        return;
    }

    summary_.max = id_to_key[max_key_id];


    summary_data_start = offset;
}

uint64_t SSTableComp::findDataOffset(const std::string& key, bool& found) const
{
    if (key < summary_.min || key > summary_.max) {
        found = false;
        return 0;
    }

    uint64_t file_offset = summary_data_start;
    
    bool error;
    
    uint64_t last_offset = 0;
    
    uint32_t r_key_id;
    string r_key;
    for(int i = 0; i < summary_.count; i++){
        readNumValue<uint32_t>(r_key_id, file_offset, summaryFile_);

        r_key = id_to_key[r_key_id];

        if(r_key > key) {
            break;
        }

        readNumValue<uint64_t>(last_offset, file_offset, summaryFile_);
    }

    file_offset = last_offset;
    uint64_t off = 0;

    while (true) {
        readNumValue<uint32_t>(r_key_id, file_offset, indexFile_);
        
        r_key = id_to_key[r_key_id];

        readNumValue<uint64_t>(off, file_offset, indexFile_);

        if (r_key == key) {
            found = true;
            return off;
        }
        if (r_key > key) {
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
    size_t val_offset = 0;

    dst = 0;

    do {
        if (!readBytes(&chunk, 1, fileOffset, fileName)) {
            return false;
        };
        finished = varenc::decodeVarint<UInt>(chunk, dst, val_offset);
    } while (finished != true);
    return true;
}
