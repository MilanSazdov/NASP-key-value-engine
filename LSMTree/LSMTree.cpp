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
namespace fs = std::filesystem;

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
