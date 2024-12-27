#include "LSMTree.h"

#include <fstream>
#include <sstream>
#include <iostream>
#include <filesystem>
#include <algorithm>
#include <cstring>
#include <cstdio>
#include <cmath>
#include <cassert>

using namespace std;
namespace fs = filesystem;

LSM::LSM(int maxL, int maxS) : maxLevel(maxL), maxSize(maxS) {}

//Check if the number of SSTables at a level are at maximum size
pair<bool, vector<string>>
LSM::IsCompactionNeeded(const string& dir,
    int level,
    vector<string>& dataFiles,
    vector<string>& indexFiles,
    vector<string>& summaryFiles,
    vector<string>& filterFiles,
    vector<string>& tocFiles,
    vector<string>& metadataFiles)
{
    // Populate vectors with matching files for given level
    FindFiles(dir, level, dataFiles, indexFiles, summaryFiles, filterFiles, tocFiles, metadataFiles);

    // Check if we have exactly maxSize index files, i.e. compaction needed
    bool compaction = (static_cast<int>(indexFiles.size()) == maxSize);

    // Return whether we need compaction, plus data files as a convenience
    return { compaction, dataFiles };
}

//Merges pairs of SStables from a level to the next level
void LSM::DoLeveledCompaction(const string& dir, int level)
{
    //If we are at the max level or above it, no furhter compaction is needed
    if (level >= maxLevel) {
        return;
    }

    vector<string> dataFiles, indexFiles, summaryFiles, filterFiles, tocFiles, metadataFiles;

    // Check if compaction is needed and get data files
    auto [compaction, dataFilesOut] = IsCompactionNeeded(
        dir, level, dataFiles, indexFiles, summaryFiles, filterFiles, tocFiles, metadataFiles
    );

    if (!compaction) {
        return;
    }

    //Check how many files exist at the next level
    vector<string> dataFilesLvlUp, indexFilesLvlUp, summaryFilesLvlUp, filterFilesLvlUp,
        tocFilesLvlUp, metadataFilesLvlUp;
    FindFiles(dir, level + 1, dataFilesLvlUp, indexFilesLvlUp, summaryFilesLvlUp, filterFilesLvlUp, tocFilesLvlUp, metadataFilesLvlUp);

    //Determine the starting number for new merged file at the next level
    int numFile = 0;
    if (indexFilesLvlUp.empty()) {
        numFile = 1;
    }
    else {
        numFile = static_cast<int>(indexFilesLvlUp.size()) + 1;
    }

    //Merge pairs of files from current level
    int i = 0;
    while (i < maxSize) {
        string fDFile = dataFiles[i];
        string fIFile = indexFiles[i];
        string fSFile = summaryFiles[i];
        string fFFile = filterFiles[i];
        string fTFile = tocFiles[i];
        string fMFile = metadataFiles[i];

        string sDFile = dataFiles[i + 1];
        string sIFile = indexFiles[i + 1];
        string sSFile = summaryFiles[i + 1];
        string sFFile = filterFiles[i + 1];
        string sTFile = tocFiles[i + 1];
        string sMFile = metadataFiles[i + 1];

        MergeLeveled(
            dir,
            fDFile, fIFile, fSFile, fFFile, fTFile, fMFile,
            sDFile, sIFile, sSFile, sFFile, sTFile, sMFile,
            level, numFile
        );

        i += 2;
        numFile++;
    }

    //After merging all pairs at a level, check the next level
    DoLeveledCompaction(dir, level + 1);
}

//Merge two SSTables at a level ino the next level
void LSM::MergeLeveled(
    const string& dir,
    const string& fDFile, const string& fIFile,
    const string& fSFile, const string& fFFile,
    const string& fTFile, const string& fMFile,
    const string& sDFile, const string& sIFile,
    const string& sSFile, const string& sFFile,
    const string& sTFile, const string& sMFile,
    int level, int numFile
)
{
    int nextLevel = level + 1;
    ostringstream oss;
    oss << dir << "-Data.db" << numFile << "-lev" << nextLevel << "-";
    string generalFilename = oss.str();

    //The data.db of tthe new merged SSTable
    string newDataFilename = generalFilename + "-Data.db";

    //Create the new datta file
    fstream newData(newDataFilename, ios::out | ios::binary);
    if (!newData.is_open()) {
        cerr << "Failed to create new data file: " << newDataFilename << endl;
        return;
    }

    //8-byte placeholder for file length, i.e. number of keys
    {
        uint64_t placeholder = 0;
        newData.write(reinterpret_cast<const char*>(&placeholder), sizeof(placeholder));
    }
    newData.close();

    newData.open(newDataFilename, ios::in | ios::out | ios::binary);
    fstream file1(dir + fDFile, ios::in | ios::binary);
    fstream file2(dir + sDFile, ios::in | ios::binary);
    if (!file1.is_open() || !file2.is_open()) {
        cerr << "Can't open one of the input data files!\n";
        return;
    }

    uint64_t fileLen1 = 0, fileLen2 = 0;
    file1.read(reinterpret_cast<char*>(&fileLen1), sizeof(fileLen1));
    file2.read(reinterpret_cast<char*>(&fileLen2), sizeof(fileLen2));

    uint64_t currentOffset = 8, currentOffset1 = 8, currentOffset2 = 8;

    uint64_t fileLen = ReadAndWrite(
        currentOffset, currentOffset1, currentOffset2,
        newData, file1, file2, fileLen1, fileLen2,
        generalFilename, level
    );
   
    newData.close();
    FileSize(newDataFilename, fileLen);

    fs::remove(dir + fDFile);
    fs::remove(dir + fIFile);
    fs::remove(dir + fSFile);
    fs::remove(dir + fFFile);
    fs::remove(dir + fTFile);
    fs::remove(dir + fMFile);

    fs::remove(dir + sDFile);
    fs::remove(dir + sIFile);
    fs::remove(dir + sSFile);
    fs::remove(dir + sFFile);
    fs::remove(dir + sTFile);
    fs::remove(dir + sMFile);
}

void LSM::DoSizeTieredCompaction(const string& dir)
{
    vector<string> allDataFiles;
    try {
        for (auto& entry : fs::directory_iterator(dir)) {
            if (!entry.is_regular_file()) continue;
            string fname = entry.path().filename().string();
            if (fname.find("-Data.db") != string::npos) {
                allDataFiles.push_back(fname);
            }
        }
    } 
    catch (const exception& e) {
        cerr << "Error scanning directory for size-tiered compaction: " << e.what() << endl;
    }

    if (allDataFiles.size() < 2) {
        return;
    }

    //Helper lamda function to get file size
    auto getSize = [&](const string& filename) -> uint64_t {
        try {
            auto fpath = fs::path(dir) / filename;
            return fs::file_size(fpath);
        }
        catch (...) {
            return UINT64_MAX;
        }
    };

    //Keep merging the two smallest until fewer than two are left
    while (allDataFiles.size() >= 2) {
        sort(allDataFiles.begin(), allDataFiles.end(), [&](const string& a, const string& b) {
            return getSize(a) < getSize(b);
            });

        string fA = allDataFiles[0];
        string fB = allDataFiles[1];

        auto baseNameA = fA.substr(0, fA.find("Data.db"));
        auto baseNameB = fB.substr(0, fB.find("Data.db"));

        string fAIndex = baseNameA + "-Index.db";
        string fASummary = baseNameA + "-Summary.db";
        string fAFilter = baseNameA + "-Filter.db";
        string fAToc = baseNameA + "-TOC.txt";
        string fAMeta = baseNameA + "-Metadata.db";

        string fBIndex = baseNameB + "-Index.db";
        string fBSummary = baseNameB + "-Summary.db";
        string fBFilter = baseNameB + "-Filter.db";
        string fBToc = baseNameB + "-TOC.txt";
        string fBMeta = baseNameB + "-Metadata.db";

        static int mergeCounter = 1;
        ostringstream oss;
        oss << dir << "merged-ST-" << (mergeCounter++) << "-Data.db";
        string newDataFile = oss.str();
        string newBaseName = newDataFile.substr(0, newDataFile.find("Data.db"));

        MergeSizeTiered(
            dir,
            fA, fAIndex, fASummary, fAFilter, fAToc, fAMeta,
            fB, fBIndex, fBSummary, fBFilter, fBToc, fBMeta,
            newBaseName
        );

        allDataFiles.erase(allDataFiles.begin());
        allDataFiles.erase(allDataFiles.begin());

        string justFile = fs::path(newDataFile).filename().string();
        allDataFiles.push_back(justFile);
    }
}

//Merges two arbitrary SSTables into a new one
void LSM::MergeSizeTiered(
    const string& dir,
    const string& fDFile, const string& fIFile,
    const string& fSFile, const string& fFFile,
    const string& fTFile, const string& fMFile,
    const string& sDFile, const string& sIFile,
    const string& sSFile, const string& sFFile,
    const string& sTFile, const string& sMFile,
    const string& newBasename
)
{
    string newDataFilename = newBasename + "-Data.db";
    fstream newData(newDataFilename, ios::out | ios::binary);
    if (!newData.is_open()) {
        cerr << "Failed to create new data file: " << newDataFilename << endl;
        return;
    }

    //8 byte placeholder for number of records
    {
        uint64_t placeholder = 0;
        newData.write(reinterpret_cast<const char*>(&placeholder), sizeof(placeholder));
    }
    newData.close();

    newData.open(newDataFilename, ios::in | ios::binary);

    fstream file1(dir + fDFile, ios::in | ios::binary);
    fstream file2(dir + sDFile, ios::in | ios::binary);
    if (!file1.is_open() || !file2.is_open()) {
        cerr << "Can't open input data files for size-tiered merge!\n";
        return;
    }

    uint64_t fileLen1 = 0, fileLen2 = 0;
    file1.read(reinterpret_cast<char*>(&fileLen1), sizeof(fileLen1));
    file2.read(reinterpret_cast<char*>(&fileLen2), sizeof(fileLen2));
    
    uint64_t currentOffset = 8, currentOffset1 = 8, currentOffset2 = 8;

    uint64_t fileLen = ReadAndWrite(
        currentOffset,
        currentOffset1,
        currentOffset2,
        newData,
        file1,
        file2,
        fileLen1,
        fileLen2,
        newBasename,
        0
    );
    newData.close();
    FileSize(newDataFilename, fileLen);

    fs::remove(dir + fDFile);
    fs::remove(dir + fIFile);
    fs::remove(dir + fSFile);
    fs::remove(dir + fFFile);
    fs::remove(dir + fTFile);
    fs::remove(dir + fMFile);

    fs::remove(dir + sDFile);
    fs::remove(dir + sIFile);
    fs::remove(dir + sSFile);
    fs::remove(dir + sFFile);
    fs::remove(dir + sTFile);
    fs::remove(dir + sMFile);

}

//Shared functions for both compaction algorithms
uint64_t LSM::ReadAndWrite(
    uint64_t& currentOffset,
    uint64_t& currentOffset1,
    uint64_t& currentOffset2,
    fstream& newData,
    fstream& fDataFile,
    fstream& sDataFile,
    uint64_t fileLen1,
    uint64_t fileLen2,
    const string& generalFilename,
    int level
)
{
    //Merged keys, offsets, values for building index, BF, Merkle tree...
    vector<string> keys;
    vector<uint64_t> offsets;
    vector<vector<char>> values;

    vector<char> crc1(4, 0);
    string timestamp1;
    unsigned char tombstone1 = 0;
    uint64_t keyLen1 = 0, valueLen1 = 0;
    string key1, value1;

    vector<char> crc2(4, 0);
    string timestamp2;
    unsigned char tombstone2 = 0;
    uint64_t keyLen2 = 0, valueLen2 = 0;
    string key2, value2;

    uint64_t firstCounter = 0;
    uint64_t secondCounter = 0;

    bool ok1 = ReadData(fDataFile, currentOffset1, crc1, timestamp1, tombstone1, keyLen1, valueLen1, key1, value1);
    bool ok2 = ReadData(fDataFile, currentOffset1, crc2, timestamp2, tombstone2, keyLen2, valueLen2, key2, value2);
    
    while (true) {
        //If file1 or file2 is finished or we have read all records
        if (!ok1 && !ok2) {
            break;
        }
        if (firstCounter == fileLen1 && secondCounter == fileLen2) {
            break;
        }

        //If file1 is finished, copy from file2
        if (!ok1 || firstCounter == fileLen1) {
            if (tombstone2 == 0) {
                offsets.push_back(currentOffset);
                currentOffset = WriteData(newData, currentOffset, crc2, timestamp2, tombstone2, keyLen2, valueLen2, key2, value2);
                keys.push_back(key2);
                values.emplace_back(value2.begin(), value2.end());
            }
            secondCounter++;
            if (secondCounter < fileLen2) {
                ok2 = ReadData(sDataFile, currentOffset2, crc2, timestamp2, tombstone2, keyLen2, valueLen2, key2, value2);
            }
            else {
                ok2 = false;
            }
            continue;
        }

        //If file2 is finished, copy from file1
        if (!ok1 && !ok2) {
            break;
        }
        if (firstCounter == fileLen1 && secondCounter == fileLen2) {
            break;
        }

        //If file1 is finished, copy from file2
        if (!ok2 || secondCounter == fileLen2) {
            if (tombstone1 == 0) {
                offsets.push_back(currentOffset);
                currentOffset = WriteData(newData, currentOffset, crc1, timestamp1, tombstone1, keyLen1, valueLen1, key1, value1);
                keys.push_back(key1);
                values.emplace_back(value1.begin(), value1.end());
            }
            firstCounter++;
            if (firstCounter < fileLen1) {
                ok1 = ReadData(sDataFile, currentOffset1, crc1, timestamp1, tombstone1, keyLen1, valueLen1, key1, value1);
            }
            else {
                ok1 = false;
            }
            continue;
        }

        //If both files have data, compare keys
        if (key1 == key2) {
            if (timestamp1 > timestamp2) {
                if (tombstone1 == 0) {
                    offsets.push_back(currentOffset);
                    currentOffset = WriteData(newData, currentOffset, crc1, timestamp1,
                        tombstone1, keyLen1, valueLen1, key1, value1);
                    keys.push_back(key1);
                    values.emplace_back(value1.begin(), value1.end());
                }
            }
            else {
                if (tombstone2 == 0) {
                    offsets.push_back(currentOffset);
                    currentOffset = WriteData(newData, currentOffset, crc2, timestamp2,
                        tombstone2, keyLen2, valueLen2, key2, value2);
                    keys.push_back(key2);
                    values.emplace_back(value2.begin(), value2.end());
                }
            }
            firstCounter++;
            secondCounter++;
            if (firstCounter < fileLen1) {
                ok1 = ReadData(fDataFile, currentOffset1, crc1, timestamp1, tombstone1, keyLen1, valueLen1, key1, value1);
            }
            else {
                ok1 = false;
            }
            if (secondCounter < fileLen2) {
                ok2 = ReadData(sDataFile, currentOffset2, crc2, timestamp2, tombstone2, keyLen2, valueLen2, key2, value2);
            }
            else {
                ok2 = false;
            }
        }
        else if (key1 < key2) {
            if (tombstone1 == 0) {
                offsets.push_back(currentOffset);
                currentOffset = WriteData(newData, currentOffset, crc1, timestamp1,
                    tombstone1, keyLen1, valueLen1, key1, value1);
                keys.push_back(key1);
                values.emplace_back(value1.begin(), value1.end());
            }
            firstCounter++;
            if (firstCounter < fileLen1) {
                ok1 = ReadData(fDataFile, currentOffset1, crc1, timestamp1, tombstone1, keyLen1, valueLen1, key1, value1);
            }
            else {
                ok1 = false;
            }
        }
        //key1 > key2
        else { 
            if (tombstone2 == 0) {
                offsets.push_back(currentOffset);
                currentOffset = WriteData(newData, currentOffset, crc2, timestamp2,
                    tombstone2, keyLen2, valueLen2, key2, value2);
                keys.push_back(key2);
                values.emplace_back(value2.begin(), value2.end());
            }
            secondCounter++;
            if (secondCounter < fileLen2) {
                ok2 = ReadData(sDataFile, currentOffset2, crc2, timestamp2, tombstone2, keyLen2, valueLen2, key2, value2);
            }
            else {
                ok2 = false;
            }
        }
    }

    //Build index
    auto idx = CreateIndex(keys, offsets, generalFilename + "-Index.db");
    idx.Write();

    //Build summary
    WriteSummary(keys, offsets, generalFilename + "-Summary.db");

    //Write BF
    WriteBloomFilter(generalFilename + "-Filter.db", keys);

    //Write TOC
    WriteTOC(generalFilename + "TOC.txt");

    //Create Merkle Tree(metadata)
    CreateMerkle(level, generalFilename + "-Data.db", values);

    //Return how many records were written down
    return static_cast<uint64_t>(keys.size());
}

uint64_t LSM::WriteData(
    fstream& file,
    uint64_t currentOffset,
    const vector<char>& crcBytes,
    const string& timestamp,
    unsigned char tombstone,
    uint64_t keyLen,
    uint64_t valueLen,
    const string& key,
    const string& value
)
{
    if (tombstone == 1) {
        return currentOffset;
    }

    file.seekp(static_cast<streamoff>(currentOffset), ios::beg);

    //CRC 4 bytes
    file.write(crcBytes.data(), 4);
    currentOffset += 4;
    
    //Timestamp 19 bytes
    string tsPadded = timestamp;
    tsPadded.resize(19, '\0');
    file.write(tsPadded.data(), 19);
    currentOffset += 19;

    //Tombstone 1 byte
    file.write(reinterpret_cast<const char*>(&tombstone), 1);
    currentOffset += 1;

    //keyLen 8 bytes
    file.write(reinterpret_cast<const char*>(&keyLen), sizeof(keyLen));
    currentOffset += 8;

    //valueLen 8 bytes
    file.write(reinterpret_cast<const char*>(&valueLen), sizeof(valueLen));
    currentOffset += 8;

    //Key
    file.write(key.data(), key.size());
    currentOffset += key.size();

    //Value
    file.write(value.data(), value.size());
    currentOffset += value.size();

    return currentOffset;
}

bool LSM::ReadData(
    fstream& file,
    uint64_t& currentOffset,
    vector<char>& crcBytes,
    string& timestamp,
    unsigned char& tombstone,
    uint64_t& keyLen,
    uint64_t& valueLen,
    string& key,
    string& value
)
{
    file.seekg(static_cast<streamoff>(currentOffset), ios::beg);

    //CRC 4 bytes
    if (!file.read(crcBytes.data(), 4)) {
        return false;
    }
    currentOffset += 4;

    //Timestamp 19 bytes
    {
        char tsBuf[19];
        if (!file.read(tsBuf, 19)) {
            return false;
        }
        timestamp.assign(tsBuf, 19);
    }
    currentOffset += 19;

    //Tombstone 1 byte
    {
        char t;
        if (!file.read(&t, 1)) {
            return false;
        }
        tombstone = static_cast<unsigned char>(t);
    }
    currentOffset += 1;

    //keyLen 8 bytes
    if (!file.read(reinterpret_cast<char*>(&keyLen), sizeof(keyLen))); {
        return false;
    }
    currentOffset += 8;

    //valueLen 8 bytes
    if (!file.read(reinterpret_cast<char*>(&valueLen), sizeof(valueLen))); {
        return false;
    }
    currentOffset += 8;

    //Key
    if (keyLen > 0) {
        vector<char> keyBuf(keyLen, 0);
        if (!file.read(keyBuf.data(), keyLen)) {
            return false;
        }
        key.assign(keyBuf.begin(), keyBuf.end());
    }
    else {
        key.clear();
    }
    currentOffset += keyLen;

    //Value
    if (valueLen > 0) {
        vector<char> valBuf(valueLen, 0);
        if (!file.read(valBuf.data(), valueLen)) {
            return false;
        }
        value.assign(valBuf.begin(), valBuf.end());
    }
    else {
        value.clear();
    }
    currentOffset += valueLen;

    return true;
}

//Overwrite first 8 bytes with 'len'
void LSM::FileSize(const string& filename, uint64_t len)
{
    fstream file(filename, ios::in | ios::out | ios::binary);
    if (!file.is_open()) {
        cerr << "Can't open file tot set file size: " << filename << endl;
        return;
    }
    file.seekp(0, ios::beg);
    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
    file.close();
}

//Find files for leveled compaction
void LSM::FindFiles(
    const string& dir,
    int level,
    vector<string>& dataFiles,
    vector<string>& indexFiles,
    vector<string>& summaryFiles,
    vector<string>& filterFiles,
    vector<string>& tocFiles,
    vector<string>& metadataFiles)
{
    string levStr = "lev" + to_string(level) + "-";

    try {
        for (auto& entry : fs::directory_iterator(dir)) {
            if (!entry.is_regular_file()) continue;
            string fname = entry.path().filename().string();
            if (fname.find(levStr + "-Data.db") != string::npos) {
                dataFiles.push_back(fname);
            }
            else if (fname.find(levStr + "-Index.db") != string::npos) {
                summaryFiles.push_back(fname);
            }
            else if (fname.find(levStr + "-Summary.db") != string::npos) {
                summaryFiles.push_back(fname);
            }
            else if (fname.find(levStr + "-TOC.txt") != string::npos) {
                tocFiles.push_back(fname);
            }
            else if (fname.find(levStr + "-Filter.db") != string::npos) {
                filterFiles.push_back(fname);
            }
            else if (fname.find(levStr + "-Metadata.db") != string::npos) {
                metadataFiles.push_back(fname);
            }
        }
    }
    catch (exception& e) {
        cerr << "Error in FindFiles: " << e.what() << endl;
    }

    sort(dataFiles.begin(), dataFiles.end());
    sort(indexFiles.begin(), indexFiles.end());
    sort(summaryFiles.begin(), summaryFiles.end());
    sort(tocFiles.begin(), tocFiles.end());
    sort(filterFiles.begin(), filterFiles.end());
    sort(metadataFiles.begin(), metadataFiles.end());
}



//Adjustments to be made

void LSM::CreateMerkle(int level, const string& newData, const vector<vector<char>>& values)
{
    string metaDir = "./kv-system/data/metadata/";
    if (fs::exists(metaDir)) {
        for (auto& entry : fs::directory_iterator(metaDir)) {
            string fname = entry.path().filename().string();
            string target = "lev" + to_string(level) + "-Metadata.txt";
            if (fname.find(target) != string::npos) {
                fs::remove(entry.path());
            }
        }
    }
    CreateMerkleTree(values, newData);
}

void LSM::CreateMerkleTree(const vector<vector<char>>& values, const string& filename)
{
    string metaDir = "./kv-system/data/metadata/";
    if (!fs::exists(metaDir)) {
        fs::create_directories(metaDir);
    }

    string outPath = metaDir + "LSM-MERKLE-" + to_string(hash<string>{}(filename)) + ".txt";
    ofstream ofs(outPath);
    if (!ofs.is_open()) {
        cerr << "Cannot open merkle metadata file: " << outPath << endl;
        return;
    }
    ofs << "MERKLE TREE STUB for: " << filename << "\n";
    ofs << "Number of values: " << values.size() << "\n";
    ofs.close();
}

void LSM::WriteSummary(const vector<string>& keys,
    const vector<uint64_t>& offsets,
    const string& summaryFilename)
{
    ofstream ofs(summaryFilename, ios::binary);
    for (size_t i = 0; i < keys.size(); i++) {
        uint64_t klen = keys[i].size();
        ofs.write(reinterpret_cast<const char*>(&klen), sizeof(klen));
        ofs.write(keys[i].data(), klen);
        ofs.write(reinterpret_cast<const char*>(&offsets[i]), sizeof(offsets[i]));
    }
    ofs.close();
}

void LSM::WriteTOC(const string& baseFilename)
{
    ofstream ofs(baseFilename);
    ofs << baseFilename << "Data.db\n";
    ofs << baseFilename << "Index.db\n";
    ofs << baseFilename << "Summary.db\n";
    ofs << baseFilename << "Filter.gob\n";
    ofs.close();
}

void LSM::WriteBloomFilter(const string& filterFilename, const vector<string>& keys)
{
    ofstream ofs(filterFilename, ios::binary);
    for (auto& k : keys) {
        ofs << k << "\n";
    }
    ofs.close();
}


LSM::Index LSM::CreateIndex(const vector<string>& keys,
    const vector<uint64_t>& offs,
    const string& indexFilename)
{
    return LSM::Index(keys, offs, indexFilename);
}
