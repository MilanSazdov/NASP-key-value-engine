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
    uint32_t& nextId)
    : SSTable(dataFile, indexFile, filterFile, summaryFile, metaFile, bmp),
    key_to_id(map),
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

bool SSTableComp::validate() {
    std::cout << "[SSTableComp] Pokrecem naprednu validaciju za: " << dataFile_ << std::endl;

    readMetaFromFile();
    if (rootHash_.empty()) {
        std::cout << "[SSTableComp] Metapodaci ne postoje. Validacija ce proci samo ako je i data fajl prazan." << std::endl;

        bool error = false;
        auto block_data = bmp->read_block({ 0, dataFile_ }, error);
        if (error || block_data.empty()) {
            std::cout << "[SSTableComp] VALIDACIJA USPEŠNA: Ni metapodaci ni podaci ne postoje." << std::endl;
            return true;
        }
        else {
            std::cerr << "[SSTableComp] GREŠKA: Data fajl postoji, ali za njega nisu sačuvani Merkle metapodaci." << std::endl;
            return false;
        }
    }

    std::vector<std::string> data_for_merkle;
    uint64_t current_offset = 0;
    std::string current_key;

    while (true) {

        uint64_t peek_offset = current_offset;
        char peek_byte;
        if (!readBytes(&peek_byte, 1, peek_offset, dataFile_)) {
            break;
        }

        std::string reconstructed_value;
        Wal_record_type flag;
        bool is_first_chunk = true;

        do {
            Record chunk_header;

            if (!readBytes(&flag, sizeof(flag), current_offset, dataFile_)) goto end_of_validation_loop;
            if (!readBytes(&chunk_header.tombstone, sizeof(chunk_header.tombstone), current_offset, dataFile_)) goto end_of_validation_loop;
            if (!readNumValue(chunk_header.crc, current_offset, dataFile_)) goto end_of_validation_loop;
            if (!readNumValue(chunk_header.timestamp, current_offset, dataFile_)) goto end_of_validation_loop;

            if (is_first_chunk) {
                uint32_t key_id = 0;
                if (!readNumValue(key_id, current_offset, dataFile_)) goto end_of_validation_loop;
                if (key_id >= id_to_key.size()) {
                    std::cerr << "GREŠKA: Pronadjen nevalidan key_id u data fajlu." << std::endl;
                    return false;
                }
                current_key = id_to_key[key_id];
            }

            if (static_cast<int>(chunk_header.tombstone) != 1) {
                uint64_t value_chunk_size = 0;
                if (!readNumValue(value_chunk_size, current_offset, dataFile_)) goto end_of_validation_loop;

                if (value_chunk_size > 0) {
                    std::string value_part(value_chunk_size, '\0');
                    if (!readBytes(&value_part[0], value_chunk_size, current_offset, dataFile_)) goto end_of_validation_loop;
                    reconstructed_value.append(value_part);
                }
            }
            is_first_chunk = false;

        } while (flag == Wal_record_type::FIRST || flag == Wal_record_type::MIDDLE);

        data_for_merkle.push_back(current_key + reconstructed_value);
    }

end_of_validation_loop:;


    MerkleTree new_merkle_tree(data_for_merkle);
    std::string new_root_hash = new_merkle_tree.getRootHash();


    if (new_root_hash == rootHash_) {
        std::cout << "[SSTableComp] VALIDACIJA USPEŠNA. Integritet podataka je potvrđen." << std::endl;
        return true;
    }


    std::cerr << "[SSTableComp] GREŠKA: VALIDACIJA NEUSPEŠNA! Integritet podataka je narušen." << std::endl;
    std::cerr << "  Originalni Hash: " << rootHash_ << std::endl;
    std::cerr << "  Trenutni Hash:   " << new_root_hash << std::endl;

    auto new_leaves = new_merkle_tree.getLeaves();

    if (new_leaves.size() != originalLeafHashes_.size()) {
        std::cerr << "  DETEKCIJA: Broj zapisa je promenjen! Originalno: "
            << originalLeafHashes_.size() << ", Trenutno: " << new_leaves.size() << std::endl;
        return false;
    }

    for (size_t i = 0; i < originalLeafHashes_.size(); ++i) {
        if (originalLeafHashes_[i] != new_leaves[i]) {
            std::cerr << "  DETEKCIJA: Prva promena u sadržaju je detektovana na zapisu sa indeksom " << i << "." << std::endl;
            return false;
        }
    }

    std::cerr << "  DETEKCIJA: Nepoznata greška. Listovi se poklapaju, ali koreni ne (ovo ne bi smelo da se desi)." << std::endl;
    return false;
}


// Funkcija NE PROVERAVA bloomfilter, pretpostavlja da je vec provereno
vector<Record> SSTableComp::get(const std::string& key)
{
    prepare(); // Citamo TOC (sparsity, offsete, itd.) i header summary fajla
    vector<Record> matches;

    // pretraga -> offset najblizeg levog rekorda
    bool maybe_in_file;
    uint64_t fileOffset = findDataOffset(key, maybe_in_file);
    if (!maybe_in_file)
    {
        return matches;
    }

    size_t header_len = 0;

    // 4) Otvorimo dataFile, idemo od fileOffset redom
    while (true) {
        if (block_size - (fileOffset % block_size) <
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

// Funkcija vraca do `n` rekorda sa razlicitim kljucem, pocinje pretragu od `key`. Vratice manje od `n` ako dodje do kraja SSTabele.
vector<Record> SSTableComp::get(const std::string& key, int n)
{
    prepare(); // Citamo TOC (sparsity, offsete, itd.) i header summary fajla
    vector<Record> ret;

    // pretraga -> offset najblizeg levog rekorda
    bool maybe_in_file;
    uint64_t fileOffset = findDataOffset(key, maybe_in_file);
    if (!maybe_in_file)
    {
        return ret;
    }

    size_t header_len = 0;

    uint64_t latest_record_timestamp = 0;
    Record latest_record;

    string last_seen_key;

    int current_count = 0;


    // 4) Otvorimo dataFile, idemo od fileOffset redom
    while (true) {
        if (block_size - (fileOffset % block_size) <
            sizeof(uint) + sizeof(ull) + 1 + 1 + sizeof(uint64_t) + sizeof(uint32_t)) {
            fileOffset += block_size - (fileOffset % block_size);
        } // Ako nema mesta za header u najgorem slucaju, paddujemo i to treba da se preskoci.
          // Lose resenje, trebalo bi da se cita index i preskoci na sledeci offset, ali ubicu se ako jos to treba da napisem

        // citamo polja Record-a
        uint crc = 0;
        uint64_t start = fileOffset;

        // Gledamo da li smo izasli iz dela za data
        if (is_single_file_mode_ && fileOffset + sizeof(uint) > toc.index_offset) {
            break;
        }

        // Gledamo da li smo dosli do kraja fajla
        if (!readNumValue<uint>(crc, fileOffset, dataFile_)) break;
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

        if (rkey >= key) {

            // Ucitamo ceo record ako je bio podeljen
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

            // Prelazimo na novi record
            if (rkey != last_seen_key && !last_seen_key.empty()) {
                if (!(bool)latest_record.tombstone) {
                    ret.emplace_back(latest_record);
                    if (++current_count == n) break;
                    latest_record_timestamp = 0;
                }
            }

            // Ako je noviji record, menjamo ts i latest_record. Inace, odbacujemo
            if (latest_record_timestamp < ts) {
                latest_record_timestamp = ts;
                latest_record = r;
            }

            last_seen_key = rkey;
        }

    }

    // Posledji record
    if ((int)ret.size() < n &&
        !last_seen_key.empty() &&
        !(bool)latest_record.tombstone)
    {
        ret.emplace_back(latest_record);
    }

    return ret;
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
        if (remaining < sizeof(r.crc) + sizeof(r.timestamp) + sizeof(char) + sizeof(r.tombstone) + sizeof(r.value_size) + sizeof(uint32_t))
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

        }
    }

    if (!concat.empty())
        bmp->write_block({ block_id, dataFile_ }, concat);

    toc.index_offset = (block_id + 1) * block_size; //TODO: proveri, trebalo bi da radi

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

    toc.summary_offset = (block_id + 1) * block_size;

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

    toc.filter_offset = (block_id + 1) * block_size;
}

void SSTableComp::readMetaFromFile() {
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

    if (content.empty()) {
        return; // Nema sadrzaja, nema sta da se parsira
    }

    // Ukloni padding karaktere sa kraja celokupnog sadrzaja
    size_t end_pos = content.find(static_cast<char>(padding_character));
    if (end_pos != std::string::npos) {
        content.erase(end_pos);
    }

    // Parsiramo procitani sadrzaj liniju po liniju
    std::stringstream ss(content);
    std::string line;

    // Prva linija je uvek root hash.
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

    std::cout << "[SSTable] Metapodaci ucitani. Root: " << rootHash_.size()
        << " bajtova, Broj listova: " << originalLeafHashes_.size() << std::endl;
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

    // payload.append(len_str);

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

    toc.meta_offset = (block_id + 1) * block_size;
}

void SSTableComp::writeMetaToFile() {

    uint64_t start_offset = 0;
    if (is_single_file_mode_) {
        start_offset = toc.meta_offset; // Set in summary writer
    }

    if (rootHash_.empty() || originalLeafHashes_.empty()) {
        std::cout << "[SSTable] No complete Merkle tree data to write for " << metaFile_ << std::endl;
        return;
    }
    // Kreiramo ceo payload sa prelascima u novi red radi lakseg parsiranja kasnije
    std::string payload = rootHash_ + "\n";
    for (const auto& leaf : originalLeafHashes_) {
        payload += leaf + "\n";
    }

    // upisujemo payload u blokove
    int block_id = start_offset / block_size;
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


uint64_t SSTableComp::findDataOffset(const std::string& key, bool& maybe_in_file) const
{
    maybe_in_file = true;
    if (key < summary_.min || key > summary_.max) {
        maybe_in_file = false;
        return 0;
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

        readNumValue<uint64_t>(off, file_offset, indexFile_);

        if (r_key >= key) {
            return off;
        }
        if (r_key > key) {
            return off;
        }
    }
    return off;
}

template<typename UInt>
bool SSTableComp::readNumValue(UInt& dst, uint64_t& fileOffset, string fileName) const {
    char chunk;
    bool finished = false;
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
