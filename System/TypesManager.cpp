#include "TypesManager.h"
#include "System.h"
#include <iostream>
#include <sstream>

using namespace std;

// Static member definitions
const string TypesManager::BF_KEY_PREFIX = "system_bf_";
const string TypesManager::CMS_KEY_PREFIX = "system_cms_";
const string TypesManager::HLL_KEY_PREFIX = "system_hll_";
const string TypesManager::SH_KEY_PREFIX = "system_sh_";

// TODO: Uncomment and test when read/write path is finished

TypesManager::TypesManager(System* sys) : system(sys) {
    if (!system) {
        throw invalid_argument("System pointer cannot be null");
    }
}

TypesManager::~TypesManager() {}

string TypesManager::generateStorageKey(const string& prefix, const string& userKey) {
    return prefix + userKey;
}

//bool TypesManager::keyExists(const string& storageKey) {
//    auto result = system->get(storageKey);
//    return result.has_value();
//}

// ========== Bloom Filter Operations ==========

bool TypesManager::createBloomFilter(const string& key, int n, double p) {
    string storageKey = generateStorageKey(BF_KEY_PREFIX, key);

    /*if (keyExists(storageKey)) {
        cout << "Bloom Filter with key '" << key << "' already exists.\n";
        return false;
    }*/

    cout << "Creating Bloom Filter with key: " << key << ", n: " << n << ", p: " << p << endl;

    BloomFilter bf(n, p);

    vector<byte> serializedBf= bf.serialize();

	//system->put(storageKey, serializedBf);

    return true;
}

bool TypesManager::deleteBloomFilter(const string& key) {
    string storageKey = generateStorageKey(BF_KEY_PREFIX, key);

   /* if (!keyExists(storageKey)) {
        cout << "Bloom Filter with key '" << key << "' does not exist.\n";
        return false;
    }*/

    system->del(storageKey);
    return true;
}

bool TypesManager::addToBloomFilter(const string& key, const string& value) {
    string storageKey = generateStorageKey(BF_KEY_PREFIX, key);

    /*if (keyExists(storageKey)) {
        cout << "Bloom Filter with key '" << key << "' already exists.\n";
        return false;
    }*/

    //vector<byte> serializedData = system->get(storageKey);

    //BloomFilter bf = BloomFilter::deserialize(serializedData);

    // bf.add(value);

    // string newSerializedData = bf.serialize();
    
    // system->put(storageKey, newSerializedData);

    cout << "Value '" << value << "' added to Bloom Filter '" << key << "'.\n";
    return true;
}

bool TypesManager::hasKeyInBloomFilter(const string& key, const string& value) {
    string storageKey = generateStorageKey(BF_KEY_PREFIX, key);
       
    /*if (keyExists(storageKey)) {
        cout << "Bloom Filter with key '" << key << "' already exists.\n";
        return false;
    }*/

    // vector<byte> serializedData = system->get(storageKey)

    // BloomFilter bf = BloomFilter::deserialize(serializedData.value());

    // bool exists = bf.contains(value);

    bool exists = true; // Placeholder

    cout << "Value '" << value << "' " << (exists ? "might exist" : "does not exist")
        << " in Bloom Filter '" << key << "'.\n";
    return exists;
}

// ========== Count-Min Sketch Operations ==========

bool TypesManager::createCountMinSketch(const string& key, double epsilon, double delta) {
    string storageKey = generateStorageKey(CMS_KEY_PREFIX, key);

    /*if (keyExists(storageKey)) {
        cout << "Count-Min Sketch with key '" << key << "' already exists.\n";
        return false;
    }*/

    cout << "Creating Count-Min Sketch with key: " << key
        << ", epsilon: " << epsilon << ", delta: " << delta << endl;

    CountMinSketch cms(epsilon, delta);

    vector<byte> serializedData = cms.serialize();

    // system->put(storageKey, serializedData);
    return true;
}

bool TypesManager::deleteCountMinSketch(const string& key) {
    string storageKey = generateStorageKey(CMS_KEY_PREFIX, key);

    /*if (!keyExists(storageKey)) {
        cout << "Count-Min Sketch with key '" << key << "' does not exist.\n";
        return false;
    }*/

    system->del(storageKey);
    return true;
}

bool TypesManager::addToCountMinSketch(const string& key, const string& value) {
    string storageKey = generateStorageKey(CMS_KEY_PREFIX, key);

    /*if (!keyExists(storageKey)) {
        cout << "Count-Min Sketch with key '" << key << "' does not exist.\n";
        return false;
    }*/

    // vector <byte> serializedData = system->get(storageKey);

    // CountMinSketch cms = CountMinSketch::deserialize(serializedData.value());

    // cms.add(value);

    // vector <byte> newSerializedData = cms.serialize();
    // system->put(storageKey, newSerializedData);

    cout << "Value '" << value << "' added to Count-Min Sketch '" << key << "'.\n";
    return true;
}

optional<int> TypesManager::getFromCountMinSketch(const string& key, const string& value) {
    string storageKey = generateStorageKey(CMS_KEY_PREFIX, key);

    /*if (!keyExists(storageKey)) {
    cout << "Count-Min Sketch with key '" << key << "' does not exist.\n";
    return false;
    }*/

    //vector <byte> serializedData = system->get(storageKey);

    // CountMinSketch cms = CountMinSketch::deserialize(serializedData.value());

    // int frequency = cms.query(value);

    int frequency = 0; // Placeholder

    cout << "Estimated frequency of '" << value << "' in Count-Min Sketch '" << key << "': "
		<< frequency << endl;
    return frequency;
}

// ========== HyperLogLog Operations ==========

bool TypesManager::createHyperLogLog(const string& key, int p) {
    string storageKey = generateStorageKey(HLL_KEY_PREFIX, key);

    /*if (keyExists(storageKey)) {
        cout << "HyperLogLog with key '" << key << "' already exists.\n";
        return false;
    }*/

	cout << "Creating HyperLogLog with key: " << key << ", p: " << p << endl;   

    HyperLogLog hll(p);

    vector <byte> serializedData = hll.serialize();

    // system->put(storageKey, serializedData);

    return true;
}

bool TypesManager::deleteHyperLogLog(const string& key) {
    string storageKey = generateStorageKey(HLL_KEY_PREFIX, key);

    /*if (!keyExists(storageKey)) {
        cout << "HyperLogLog with key '" << key << "' does not exist.\n";
        return false;
    }*/

    system->del(storageKey);
    return true;
}

bool TypesManager::addToHyperLogLog(const string& key, const string& value) {
    string storageKey = generateStorageKey(HLL_KEY_PREFIX, key);

    /*if (!keyExists(storageKey)) {
        cout << "HyperLogLog with key '" << key << "' does not exist.\n";
        return false;
    }*/

    // vector <byte> serializedData = system->get(storageKey);

    // HyperLogLog hll = HyperLogLog::deserialize(serializedData.value());

    // hll.add(value);

    // vector <byte> newSerializedData = hll.serialize();
    // system->put(storageKey, newSerializedData);

    cout << "Value '" << value << "' added to HyperLogLog '" << key << "'.\n";
    return true;
}

optional<size_t> TypesManager::estimateHyperLogLog(const string& key) {
    string storageKey = generateStorageKey(HLL_KEY_PREFIX, key);

    /*if (!keyExists(storageKey)) {
        cout << "HyperLogLog with key '" << key << "' does not exist.\n";
        return false;
    }*/

    //vector <byte> serializedData = system->get(storageKey);

    // HyperLogLog hll = HyperLogLog::deserialize(serializedData.value());

    // size_t cardinality = hll.estimate();

    size_t cardinality = 0; // Placeholder

    cout << "Estimated cardinality of HyperLogLog '" << key << "': " << cardinality << endl;
    return cardinality;
}

// ========== SimHash Operations ==========

bool TypesManager::addSimHashFingerprint(const string& key, const string& text) {
    string storageKey = generateStorageKey(SH_KEY_PREFIX, key);

    /*if (!keyExists(storageKey)) {
        cout << "SimHash fingerprint with key '" << key << "' does not exist.\n";
        return false;
    }*/

	cout << "Adding SimHash fingerprint for key: " << key << endl;

    SimHash sh;
    uint64_t fingerprint = sh.getFingerprint(text);

    // system->put(storageKey, serializedData);

    return true;
}

bool TypesManager::deleteSimHashFingerprint(const string& key) {
    string storageKey = generateStorageKey(SH_KEY_PREFIX, key);

    /*if (!keyExists(storageKey)) {
        cout << "SimHash fingerprint with key '" << key << "' does not exist.\n";
        return false;
    }*/

    system->del(storageKey);
    return true;
}

optional<int> TypesManager::getHammingDistance(const string& key1, const string& key2) {
    string storageKey1 = generateStorageKey(SH_KEY_PREFIX, key1);
    string storageKey2 = generateStorageKey(SH_KEY_PREFIX, key2);


    /*if (!keyExists(storageKey1)) {
        cout << "SimHash fingerprint with key '" << key1 << "' does not exist.\n";
        return false;
    }*/

    /*if (!keyExists(storageKey2)) {
        cout << "SimHash fingerprint with key '" << key2 << "' does not exist.\n";
        return false;
    }*/

	cout << "Calculating Hamming distance between '" << key1 << "' and '" << key2 << "'\n";

    auto fingerprint1Data = system->get(storageKey1);
    auto fingerprint2Data = system->get(storageKey2);

	// uint64_t fp1 = system->get(storageKey1);
    // uint64_t fp2 = system->get(storageKey2);
    // int hammingDistance = SimHash::calculateHammingDistance(fp1, fp2);

    int hammingDistance = 0; // Placeholder

    cout << "Hamming distance between '" << key1 << "' and '" << key2 << "': " << hammingDistance << endl;
    return hammingDistance;
}