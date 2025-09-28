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

TypesManager::TypesManager(System* sys) : system(sys) {
    if (!system) {
        throw invalid_argument("System pointer cannot be null");
    }
}

TypesManager::~TypesManager() {}

string TypesManager::generateStorageKey(const string& prefix, const string& userKey) {
    return prefix + userKey;
}

// string -> vector<byte>
static vector<byte> stringToBytes(const string& str) {
    vector<byte> bytes;
    bytes.reserve(str.size());
    for (char c : str) {
        bytes.push_back(static_cast<byte>(c));
    }
    return bytes;
}

// vector<byte> -> string
static string bytesToString(const vector<byte>& data) {
    return string(reinterpret_cast<const char*>(data.data()), data.size());
}

// ========== Bloom Filter Operations ==========

bool TypesManager::createBloomFilter(const string& key, int n, double p) {
    string storageKey = generateStorageKey(BF_KEY_PREFIX, key);

	// check if n is integer and positive, and p is between 0 and 1
    if (n <= 0) {
        cout << "Error: Number of elements (n) must be a positive integer.\n";
        return false;
	}
    if (p <= 0 || p >= 1) {
        cout << "Error: False-positive probability (p) must be between 0 and 1 (exclusive).\n";
        return false;
	}

    cout << "Creating Bloom Filter with key: " << key << ", n: " << n << ", p: " << p << endl;

    BloomFilter bf(n, p);
    vector<byte> serializedBf = bf.serialize();

    system->put(storageKey, bytesToString(serializedBf));
    cout << "Bloom Filter created with key: " << key
        << ", n: " << n << ", p: " << p << endl;
    return true;
}

bool TypesManager::deleteBloomFilter(const string& key) {
    string storageKey = generateStorageKey(BF_KEY_PREFIX, key);
    system->del(storageKey);
    cout << "Bloom Filter with key: " << key << " deleted" << endl;
    return true;
}

bool TypesManager::addToBloomFilter(const string& key, const string& value) {
    string storageKey = generateStorageKey(BF_KEY_PREFIX, key);

    optional<string> storedOpt = system->get(storageKey);
    if (!storedOpt.has_value()) {
        cout << "Bloom Filter with key '" << key << "' does not exist.\n";
        return false;
    }

    vector<byte> serializedData = stringToBytes(storedOpt.value());
    BloomFilter bf = BloomFilter::deserialize(serializedData);

    bf.add(value);

    vector<byte> newSerializedData = bf.serialize();
    system->put(storageKey, bytesToString(newSerializedData));

    cout << "Value '" << value << "' added to Bloom Filter '" << key << "'.\n";
    return true;
}

bool TypesManager::hasKeyInBloomFilter(const string& key, const string& value) {
    string storageKey = generateStorageKey(BF_KEY_PREFIX, key);

    optional<string> storedOpt = system->get(storageKey);
    if (!storedOpt.has_value()) {
        cout << "Bloom Filter with key '" << key << "' does not exist.\n";
        return false;
    }

    vector<byte> serializedData = stringToBytes(storedOpt.value());
    BloomFilter bf = BloomFilter::deserialize(serializedData);

    bool exists = bf.possiblyContains(value);

    cout << "Value '" << value << "' " << (exists ? "might exist" : "does not exist")
        << " in Bloom Filter '" << key << "'.\n";
    return exists;
}

// ========== Count-Min Sketch Operations ==========

bool TypesManager::createCountMinSketch(const string& key, double epsilon, double delta) {
    string storageKey = generateStorageKey(CMS_KEY_PREFIX, key);

    if (epsilon <= 0 || epsilon >= 1) {
        cout << "Error: Epsilon must be between 0 and 1 (exclusive).\n";
        return false;
	}
    if (delta <= 0 || delta >= 1) {
        cout << "Error: Delta must be between 0 and 1 (exclusive).\n";
        return false;
    }

    cout << "Creating Count-Min Sketch with key: " << key
        << ", epsilon: " << epsilon << ", delta: " << delta << endl;

    CountMinSketch cms(epsilon, delta);
    vector<byte> serializedData = cms.serialize();

    system->put(storageKey, bytesToString(serializedData));
    cout << "Count-Min Sketch created with key: " << key
        << ", epsilon: " << epsilon << ", delta: " << delta << endl;
    return true;
}

bool TypesManager::deleteCountMinSketch(const string& key) {
    string storageKey = generateStorageKey(CMS_KEY_PREFIX, key);
    system->del(storageKey);
    cout << "Count-Min Sketch with key: " << key << " deleted" << endl;
    return true;
}

bool TypesManager::addToCountMinSketch(const string& key, const string& value) {
    string storageKey = generateStorageKey(CMS_KEY_PREFIX, key);

    optional<string> storedOpt = system->get(storageKey);
    if (!storedOpt.has_value()) {
        cout << "Count-Min Sketch with key '" << key << "' does not exist.\n";
        return false;
    }

    vector<byte> serializedData = stringToBytes(storedOpt.value());
    CountMinSketch cms = CountMinSketch::deserialize(serializedData);

    cms.add(value);

    vector<byte> newSerializedData = cms.serialize();
    system->put(storageKey, bytesToString(newSerializedData));

    cout << "Value '" << value << "' added to Count-Min Sketch '" << key << "'.\n";
    return true;
}

optional<int> TypesManager::getFromCountMinSketch(const string& key, const string& value) {
    string storageKey = generateStorageKey(CMS_KEY_PREFIX, key);

    optional<string> storedOpt = system->get(storageKey);
    if (!storedOpt.has_value()) {
        cout << "Count-Min Sketch with key '" << key << "' does not exist.\n";
        return nullopt;
    }

    vector<byte> serializedData = stringToBytes(storedOpt.value());
    CountMinSketch cms = CountMinSketch::deserialize(serializedData);

    int frequency = cms.query(value);
    cout << "Estimated frequency of '" << value << "' in Count-Min Sketch '" << key << "': "
        << frequency << endl;

    return frequency;
}

// ========== HyperLogLog Operations ==========

bool TypesManager::createHyperLogLog(const string& key, int p) {
    string storageKey = generateStorageKey(HLL_KEY_PREFIX, key);

    if (p < 4 || p > 16) {
        cout << "Error: Precision (p) must be between 4 and 16.\n";
        return false;
	}

    cout << "Creating HyperLogLog with key: " << key << ", p: " << p << endl;

    HyperLogLog hll(p);
    vector<byte> serializedData = hll.serialize();

    system->put(storageKey, bytesToString(serializedData));
    cout << "HyperLogLog created with key: " << key << ", p: " << p << endl;
    return true;
}

bool TypesManager::deleteHyperLogLog(const string& key) {
    string storageKey = generateStorageKey(HLL_KEY_PREFIX, key);
    system->del(storageKey);
    cout << "HyperLogLog with key: " << key << " deleted" << endl;
    return true;
}

bool TypesManager::addToHyperLogLog(const string& key, const string& value) {
    string storageKey = generateStorageKey(HLL_KEY_PREFIX, key);

    optional<string> storedOpt = system->get(storageKey);
    if (!storedOpt.has_value()) {
        cout << "HyperLogLog with key '" << key << "' does not exist.\n";
        return false;
    }

    vector<byte> serializedData = stringToBytes(storedOpt.value());
    HyperLogLog hll = HyperLogLog::deserialize(serializedData);

    hll.add(value);

    vector<byte> newSerializedData = hll.serialize();
    system->put(storageKey, bytesToString(newSerializedData));

    cout << "Value '" << value << "' added to HyperLogLog '" << key << "'.\n";
    return true;
}

optional<size_t> TypesManager::estimateHyperLogLog(const string& key) {
    string storageKey = generateStorageKey(HLL_KEY_PREFIX, key);

    optional<string> storedOpt = system->get(storageKey);
    if (!storedOpt.has_value()) {
        cout << "HyperLogLog with key '" << key << "' does not exist.\n";
        return nullopt;
    }

    vector<byte> serializedData = stringToBytes(storedOpt.value());
    HyperLogLog hll = HyperLogLog::deserialize(serializedData);

    double cardinality = hll.estimate();
    cout << "Estimated cardinality of HyperLogLog '" << key << "': " << cardinality << endl;

    return cardinality;
}

// ========== SimHash Operations ==========

bool TypesManager::addSimHashFingerprint(const string& key, const string& text) {
    string storageKey = generateStorageKey(SH_KEY_PREFIX, key);

    cout << "Adding SimHash fingerprint for key: " << key << endl;

    SimHash sh;
    uint64_t fingerprint = sh.getFingerprint(text);

    // store fingerprint as raw bytes
    vector<byte> serializedData(sizeof(uint64_t));
    memcpy(serializedData.data(), &fingerprint, sizeof(uint64_t));

    system->put(storageKey, bytesToString(serializedData));
    cout << "Fingerprint for key '" << key << "' added successfully.\n";
    return true;
}

bool TypesManager::deleteSimHashFingerprint(const string& key) {
    string storageKey = generateStorageKey(SH_KEY_PREFIX, key);
    system->del(storageKey);
    cout << "Fingerprint with key: " << key << " deleted" << endl;
    return true;
}

optional<int> TypesManager::getHammingDistance(const string& key1, const string& key2) {
    string storageKey1 = generateStorageKey(SH_KEY_PREFIX, key1);
    string storageKey2 = generateStorageKey(SH_KEY_PREFIX, key2);

    auto text1Opt = system->get(storageKey1);
    auto text2Opt = system->get(storageKey2);

    if (!text1Opt.has_value() || !text2Opt.has_value()) {
        cout << "One or both SimHash texts do not exist.\n";
        return nullopt;
    }

    SimHash sh;
    int hammingDistance = sh.getHammingDistance(text1Opt.value(), text2Opt.value());

    cout << "Hamming distance between '" << key1 << "' and '" << key2
        << "': " << hammingDistance << endl;

    return hammingDistance;
}
