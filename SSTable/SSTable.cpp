#include "SSTable.h"
#include <iostream>

static const size_t SPARSITY = 64;

SSTable::SSTable(const std::string& dataFile,
    const std::string& indexFile,
    const std::string& filterFile,
    const std::string& summaryFile,
    const std::string& metaFile)
    : dataFile_(dataFile),
      indexFile_(indexFile),
      filterFile_(filterFile),
      summaryFile_(summaryFile),
      metaFile_(metaFile) {}

void SSTable::build(const std::vector<Record>& records)
{
    if (records.empty()) {
        std::cerr << "[SSTable] build: Nema zapisa.\n";
        return;
    }

    index_ = writeDataMetaFiles(records);

    BloomFilter bf(records.size(), 0.01); // TODO: Config
    for (auto& r : records) {
        bf.add(r.key);
    }
    bloom_ = bf;

    // Index u fajl
    vector<IndexEntry> summaryAll = writeIndexToFile();

    summary_.summary.reserve(summaryAll.size() / SPARSITY + 1);

    for (size_t i = 0; i < summaryAll.size(); i += SPARSITY) {
        summary_.summary.push_back(summaryAll[i]);
    }
    if (!summaryAll.empty() &&
        ((summaryAll.size() - 1) % SPARSITY) != 0)
    {
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
vector<Record> SSTable::get(const std::string& key)
{
    readSummaryFromFile();
    vector<Record> matches;

    // 3) binarna pretraga -> offset
    bool found;
    uint64_t startOffset = findDataOffset(key, found);
    if(!found)
    {
        return matches;
    }

    // 4) Otvorimo dataFile, idemo od startOffset redom
    std::ifstream in(dataFile_, std::ios::binary);
    if (!in.is_open()) {
        cerr<<"[SSTable] get: Problem sa ucitavanjem fajla" << endl;
        return matches;
    }
    in.seekg(startOffset, std::ios::beg);


    while (true) {
        // citamo polja Record-a
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
        

        if (rkey == key) {
            Record r;
            r.crc = crc;
            r.flag = static_cast<byte>(flag);
            r.timestamp = ts;
            r.tombstone = static_cast<byte>(tomb);
            r.key_size = kSize;
            r.value_size = vSize;
            r.key = rkey;
            r.value = rvalue;

            matches.emplace_back(r);
        }
        if (rkey > key) {
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

string SSTable::buildMerkleTree(const vector<string>& leaves)
{
    tree.clear();

    if (leaves.empty()) return "";
    std::hash<std::string> hasher;
    std::vector<std::string> currentLevel = leaves;
    while (currentLevel.size() > 1) {
        std::vector<std::string> nextLevel;

        for (size_t i = 0; i < currentLevel.size(); i += 2) {
            std::string left = currentLevel[i];
            std::string right = (i + 1 < currentLevel.size()) ? currentLevel[i + 1] : left;
            
            nextLevel.push_back(std::to_string(hasher(left+right)));
            tree.push_back(std::to_string(hasher(left+right)));

        }
        currentLevel = nextLevel;
    }

    return currentLevel.front(); //root
}

std::vector<IndexEntry>
SSTable::writeDataMetaFiles(const std::vector<Record>& sortedRecords) const
{
    std::vector<IndexEntry> ret;
    ret.reserve(sortedRecords.size());
    
    std::ofstream out(dataFile_, std::ios::binary | std::ios::out);
    if (!out.is_open()) {
        throw std::runtime_error("Ne mogu otvoriti " + dataFile_);
    }

    std::vector<std::string> hashes;
    std::hash<std::string> hasher;
    uint64_t offset = 0ULL;
    for (auto& r : sortedRecords) {
        // snimamo IndexEntry
        IndexEntry ie;
        ie.key = r.key;
        ie.offset = offset;
        ret.push_back(ie);

        // upis polja Record-a:
        std::string concat;

        concat.append(reinterpret_cast<const char*>(&r.crc), sizeof(r.crc));
        concat.append(reinterpret_cast<const char*>(&r.flag), sizeof(r.flag));
        concat.append(reinterpret_cast<const char*>(&r.timestamp), sizeof(r.timestamp));
        concat.append(reinterpret_cast<const char*>(&r.tombstone), sizeof(r.tombstone));
        concat.append(reinterpret_cast<const char*>(&r.key_size), sizeof(r.key_size));
        concat.append(reinterpret_cast<const char*>(&r.value_size), sizeof(r.value_size));
        concat.append(r.key);
        concat.append(r.value);

        std::string hashValue = std::to_string(hasher(concat));

        //TODO: Ovo sve treba da radi sa blokovima.
        //Moglo bi lako da se napravi konkatenacija svih Record-a, pa deli na blokove, ali zauzelo bi puno memorije.
        hashes.push_back(hashValue);

        out.write(concat.data(), concat.size());

        offset += concat.size();
    }

    out.close();
    return ret;
}

std::vector<IndexEntry> SSTable::writeIndexToFile()
{
    std::vector<IndexEntry> ret;
    
    std::ofstream idx(indexFile_, std::ios::binary | std::ios::out);
    if (!idx.is_open()) {
        throw std::runtime_error("Ne mogu otvoriti " + indexFile_);
    }
    
    uint64_t count = index_.size();
    idx.write(reinterpret_cast<char*>(&count), sizeof(count));

    uint64_t offset = 8ULL;

    for (auto& ie : index_) {
        IndexEntry summEntry;
        summEntry.key = ie.key;
        summEntry.offset = offset;
        ret.push_back(summEntry);
        
        uint64_t kSize = ie.key.size();
        idx.write(reinterpret_cast<char*>(&kSize), sizeof(kSize));
        idx.write(ie.key.data(), kSize);
        idx.write(reinterpret_cast<char*>(&ie.offset), sizeof(ie.offset));

        offset += 8 + kSize + 8;
    }
    idx.close();
    return ret;
}

void SSTable::writeSummaryToFile()
{
    
    std::ofstream idx(summaryFile_, std::ios::binary | std::ios::out);
    if (!idx.is_open()) {
        throw std::runtime_error("Ne mogu otvoriti " + summaryFile_);
    }

    uint64_t minKeyLength = summary_.min.size();
    idx.write(reinterpret_cast<char*>(&minKeyLength), sizeof(minKeyLength));
    idx.write(summary_.min.data(), minKeyLength);

    uint64_t maxKeyLength = summary_.max.size();
    idx.write(reinterpret_cast<char*>(&maxKeyLength), sizeof(maxKeyLength));
    idx.write(summary_.max.data(), maxKeyLength);
    
    uint64_t count = summary_.summary.size();
    idx.write(reinterpret_cast<char*>(&count), sizeof(count));

    uint64_t offset = 8 + minKeyLength + 8 + maxKeyLength + 8;

    for (auto& summEntry : summary_.summary) {
        uint64_t kSize = summEntry.key.size();
        idx.write(reinterpret_cast<char*>(&kSize), sizeof(kSize));
        idx.write(summEntry.key.data(), kSize);
        idx.write(reinterpret_cast<char*>(&summEntry.offset), sizeof(summEntry.offset));

        offset += 8 + kSize + 8;
    }
    idx.close();
}

void SSTable::writeMetaToFile() const
{
    std::ofstream metaFile(metaFile_, std::ios::binary | std::ios::out);
    if (!metaFile.is_open()) {
        cerr << "[SSTable] writeMetaToFile: Ne mogu da ucitam fajl " + metaFile_ << endl;
        return;
    }

    uint64_t treeSize = tree.size();
    metaFile.write(reinterpret_cast<const char*>(&treeSize), sizeof(treeSize));

    for (const auto& hashStr : tree) {
        if (hashStr.size() != sizeof(uint64_t)) {
            cerr << "[SSTable] writeMetaToFile: Ocekuju se vrednosti duzine 8 byte, a duzina hash-a je " + to_string(hashStr.size()) + " bajtova." << endl;
            return;
        }
        metaFile.write(hashStr.data(), sizeof(uint64_t));
    }

    metaFile.close();    
}

void SSTable::readMetaFromFile()
{
    std::ifstream metaFile(metaFile_, std::ios::binary | std::ios::in);
    if (!metaFile.is_open()) {
        cerr << "[SSTable] readMetaFromFile: Ne mogu da ucitam fajl " + metaFile_ << endl;
        return;
    }

    uint64_t treeSize;
    if (!metaFile.read(reinterpret_cast<char*>(&treeSize), sizeof(treeSize))) {
        cerr << "[SSTable] writeMetaToFile: Ne mogu da ucitam duzinu stabla" << endl;
        return;
    }

    tree.clear();
    tree.reserve(treeSize);

    for (uint64_t i = 0; i < treeSize; ++i) {
        char hashBuffer[sizeof(uint64_t)];
        if (!metaFile.read(hashBuffer, sizeof(uint64_t))) {
            cerr << "[SSTable] writeMetaToFile: Ne mogu da ucitam hash" << endl;
            return;
        }
        tree.emplace_back(hashBuffer, sizeof(uint64_t));
    }

    metaFile.close();
}

void SSTable::readIndexFromFile()
{
    // Ako index_ vec ima podatke, pretpostavimo da je ucitan (ili ga clear() pa ucitaj).
    if (!index_.empty()) {
        return;
    }
    std::ifstream idx(indexFile_, std::ios::binary);
    if (!idx.is_open()) {
        // nema index fajla
        return;
    }
    uint64_t count = 0;
    if (!idx.read(reinterpret_cast<char*>(&count), sizeof(count))) {
        return;
    }
    index_.reserve(count);
    for (uint64_t i = 0; i < count; i++) {
        uint64_t kSize;
        if (!idx.read(reinterpret_cast<char*>(&kSize), sizeof(kSize))) break;
        std::string kbuf(kSize, '\0');
        if (!idx.read(&kbuf[0], kSize)) break;

        uint64_t off;
        if (!idx.read(reinterpret_cast<char*>(&off), sizeof(off))) break;

        IndexEntry ie;
        ie.key = kbuf;
        ie.offset = off;
        index_.push_back(ie);
    }
    idx.close();
}

void SSTable::writeBloomToFile() const
{
    std::vector<uint8_t> raw = bloom_.serialize();

    std::ofstream bfout(filterFile_, std::ios::binary | std::ios::out);
    if (!bfout.is_open()) {
        throw std::runtime_error("Ne mogu otvoriti " + filterFile_);
    }

    uint64_t len = raw.size();
    bfout.write(reinterpret_cast<char*>(&len), sizeof(len));
    bfout.write(reinterpret_cast<const char*>(raw.data()), raw.size());
    bfout.close();
}

void SSTable::readBloomFromFile()
{
    std::ifstream bfin(filterFile_, std::ios::binary);
    if (!bfin.is_open()) {
        return;
    }

    uint64_t len = 0;
    if (!bfin.read(reinterpret_cast<char*>(&len), sizeof(len))) {
        return;
    }
    std::vector<uint8_t> raw(len);
    if (!bfin.read(reinterpret_cast<char*>(raw.data()), len)) {
        return;
    }
    bloom_ = BloomFilter::deserialize(raw);
    bfin.close();
}

void SSTable::readSummaryFromFile()
{
    std::ifstream idx(summaryFile_, std::ios::binary);
    if (!idx.is_open()) {
        std::cerr << "[SSTable] readSummaryFromFile: Problem sa otvaranjem fajla " + summaryFile_ << std::endl;
        return;
    }

    uint64_t minKeyLength;
    if (!idx.read(reinterpret_cast<char*>(&minKeyLength), sizeof(minKeyLength))) {
        return;
    }
    summary_.min.resize(minKeyLength);
    if (!idx.read(summary_.min.data(), minKeyLength)) {
        return;
    }

    uint64_t maxKeyLength;
    if (!idx.read(reinterpret_cast<char*>(&maxKeyLength), sizeof(maxKeyLength))) {
        return;
    }
    summary_.max.resize(maxKeyLength);
    if (!idx.read(summary_.max.data(), maxKeyLength)) {
        return;
    }

    uint64_t count;
    if (!idx.read(reinterpret_cast<char*>(&count), sizeof(count))) {
        return;
    }

    summary_.summary.reserve(count);
    for (uint64_t i = 0; i < count; ++i) {
        uint64_t kSize;
        if (!idx.read(reinterpret_cast<char*>(&kSize), sizeof(kSize))) break;

        std::string key(kSize, '\0');
        if (!idx.read(key.data(), kSize)) break;

        uint64_t offset;
        if (!idx.read(reinterpret_cast<char*>(&offset), sizeof(offset))) break;

        summary_.summary.push_back({key, offset});
    }

    idx.close();
}

uint64_t SSTable::findDataOffset(const std::string& key, bool& found) const
{
    //TODO: Block manager
    if (key < summary_.min) {
        found=false;
        return 0ULL;
    }
    if (key > summary_.max) {
        found=false;
        return 0ULL;
    }

    int left = 0, right = static_cast<int>(summary_.summary.size()) - 1;
    int ans = -1;

    while (left <= right) {
        int mid = (left + right) / 2;

        if (summary_.summary[mid].key <= key) {
            ans = mid;
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    uint64_t offset = summary_.summary[ans].offset;

    std::ifstream in(indexFile_, std::ios::binary);
    if (!in.is_open()) {
        cerr<<"[SSTable] findOffset: Problem sa ucitavanjem fajla " + indexFile_ << endl;
        found = false;
        return 0ULL;
    }
    in.seekg(offset, std::ios::beg);

    while (true) {
        uint64_t kSize;
        if (!in.read(reinterpret_cast<char*>(&kSize), sizeof(kSize))) break;
        std::string rkey(kSize, '\0');
        if (!in.read(&rkey[0], kSize)) break;

        uint64_t off;
        if (!in.read(reinterpret_cast<char*>(&off), sizeof(off))) break;

        if (rkey == key) {
            found=true;
            return off;
        }
        if (rkey > key) {
            found=false;
            return 0ULL;
        }
    }
    found=false;
    return 0ULL;
}
