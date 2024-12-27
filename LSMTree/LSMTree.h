#ifndef LSM_TREE_H
#define LSM_TREE_H

#include <string>
#include <vector>
#include <fstream>

//#include "BloomFilter.h"
//#include "MerkleTree.h"
//#include "SSTable.h"


using namespace std;

class LSM {
public:
	//Constructor
	//maxL - maximum number of levels for leveled compaction
	//maxS - maximum number of SSTables in a level before compaction is called
	LSM(int maxL, int maxS);

	//Perform compaction at a given level
	void DoLeveledCompaction(const string& dir, int level);

	//Perform size-tiered compaction by merging the 2 smallest SSTables
	void DoSizeTieredCompaction(const string& dir);

private:
	int maxLevel;
	int maxSize;

	//Helper function for leveled compaction
	//Checks if compaction is needed at a given level
	pair<bool, vector<string>> IsCompactionNeeded(
		const string& dir,
		int level,
		vector<string>& dataFiles,
		vector<string>& indexFiles,
		vector<string>& summaryFiles,
		vector<string>& filterFiles,
		vector<string>& tocFiles,
		vector<string>& metadataFiles
	);

	//Merges pairs of SSTables at the current level into a new SSTable at the next level
	void MergeLeveled(
		const string& dir,
		const string& fDFile, const string& fIFile,
		const string& fSFile, const string& fTFile,
		const string& fFFile, const string& fMFile,
		const string& sDFile, const string& sIFile,
		const string& sSFile, const string& sTFile,
		const string& sFFfile, const string& sMFile,
		int level, int numFile
	);

	//Helper function for size-tiered compaction
	//Merges two arbitrary SSTables into a new one
	void MergeSizeTiered(
		const string& dir,
		const string& fDFile, const string& fIFile,
		const string& fSFile, const string& fTFile,
		const string& fFFile, const string& fMFile,
		const string& sDFile, const string& sIFile,
		const string& sSFile, const string& sTFile,
		const string& sFFfile, const string& sMFile,
		const string& newBaseName
	);

	//Reads both data files, merges them, and writes out the new data file
	//Also updates index, summary, BloomFilter, TOC, Merkle tree, metadata
	uint64_t ReadAndWrite(
		uint64_t& currentOffset,
		uint64_t& currentOffset1,
		uint64_t& currentOffset2,
		fstream& newData,
		fstream& fDataFile,
		fstream& sDataFile,
		uint64_t fileLen1,
		uint64_t fileLen2,
		const string& generalFileName,
		int level //for merkle naming, 0 for size-tiered compaction
	);

	//Helper function to write a single record to new data file
	uint64_t WriteData(
		fstream& file,
		uint64_t currentOffset,
		const vector<char>& crcBytes,
		const string& timestamp,
		unsigned char tombstone,
		uint64_t keyLen,
		uint64_t valueLen,
		const string& key,
		const string& value
	);

	//Helper function to read a single record from an SSTable data file
	bool ReadData(
		fstream& file,
		uint64_t& currentOffset,
		vector<char>& crcBytes,
		string& timestamp,
		unsigned char& tombstone,
		uint64_t& keyLen,
		uint64_t& valueLen,
		string& key,
		string& value
	);

	//Updates the first 8 bytes of the data file with the number of records
	void FileSize(const string& filename, uint64_t len);

	//Gathers matching file names for a given level
	void FindFiles(
		const string& dir,
		int level,
		vector<string>& dataFiles,
		vector<string>& indexFiles,
		vector<string>& summaryFiles,
		vector<string>& filterFiles,
		vector<string>& tocFiles,
		vector<string>& metadataFiles
	);

	void CreateMerkle(int level, const string& newData, const vector<vector<char>>& values);
	void CreateMerkleTree(const vector<vector<char>>& values, const string& filename);

	void WriteSummary(const vector<string>& keys, vector<uint64_t>& offsets, const string& summaryFileName );

	void WriteTOC(const string& baseFilename);

	void WriteBloomFilter(const string& filterFileName, const vector<string>& keys);


	class Index {
	public:
		Index(const vector<string>& k, const vector<uint64_t>& o, const string& filename)
			: keys(k), offsets(o), indexFilename(filename) {}

		void Write() {
			ofstream ofs(indexFilename, ios::binary);
			if (!ofs) {
				throw runtime_error("Unable to open file: " + indexFilename);
			}

			uint64_t numKeys = keys.size();
			ofs.write(reinterpret_cast<const char*>(&numKeys), sizeof(numKeys));

			// Write each key and its corresponding offset
			for (size_t i = 0; i < keys.size(); i++) {
				uint64_t keyLen = keys[i].size();
				ofs.write(reinterpret_cast<const char*>(&keyLen), sizeof(keyLen));
				ofs.write(keys[i].data(), keyLen);
				ofs.write(reinterpret_cast<const char*>(&offsets[i]), sizeof(offsets[i]));
			}
			ofs.close();
		}

	private:
		vector<string> keys;
		vector<uint64_t> offsets;
		string indexFilename;
	};

	Index CreateIndex(const vector<string>& keys, const vector<uint64_t>& offs, const string& indexFillename);

};

#endif
