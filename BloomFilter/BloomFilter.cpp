#include "BloomFilter.h"

/*
    * Format zapisa:
    * m - 4 bajta, unsigned int
    * k - 4 bajta, unsigned int
    * p - 8 bajta, double
    * timeConst - 4 bajta, unsigned int
    * h2_seed - 8 bajta, size_t pretpostavljeno 64-bitno
    * bitSet bajtovi (m + 7) / 8 bajtova

    memcpy() - is used to copy a block of memory from one location to another.
    => copies the specified number of bytes from one memory location to another regardless of data type stored
    in <string.h>

    size_t - unsigned integer type, used to represent the size of objects in bytes
    * na primer sizeof() vraca size_t

    automatski se prilagodjava da aplikacija radi ispravno i na 32-bitnim i na 64-bitnim sistemima
    standardizacija: mnoge biblioteke i API-ji ocekuju size_t, pa je to onda najbolje koristiti za kompatibilnost

*/

using namespace std;

BloomFilter::BloomFilter() {};

BloomFilter::BloomFilter(unsigned int n, double falsePositiveRate) {
	// Calculate size of bit set
    m = calculateSizeOfBitSet(n, falsePositiveRate);
	// Calculate number of hash functions
    k = calculateNumberOfHashFunctions(n, m);
    p = falsePositiveRate;
	// Initialize bit set - na pocetku su svi bitovi postavljeni na 0
    bitSet = vector<bool>(m, false);
	// Seed for generating hash functions
    timeConst = static_cast<unsigned int>(time(nullptr));

	// Koristimo double hashing tehniku => trebaju nam dve hash funkcije
	// h1 = hash(key)
	// h2 = hash(to_string(h2_seed) + key)

	// Ovde sada treba generisati RAZLICITH k hash funkcija
	mt19937 rng(timeConst); // Random number generator
    uniform_int_distribution<size_t> dist(0, numeric_limits<size_t>::max());
    h2_seed = dist(rng); // Generate a random seed for the second hash function

	// Sada kreiramo k hash funkcija. Svaka ce za zadati kljuc:
	// 1) Izracunati h1 i h2
	// 2) Vratiti (h1 + i * h2) % m

    hashFunctions.reserve(k);
    unsigned int m_copy = m;
    size_t seed_copy = h2_seed;

    for (unsigned int i = 0; i < k; ++i) {
        hashFunctions.emplace_back(
            [i, m_copy, seed_copy](const string& key) {
                size_t h1 = hash<string>()(key);
                size_t h2 = hash<string>()(to_string(seed_copy) + key);
                return (h1 + i * h2) % m_copy;
            }
        );
    }
}

bool BloomFilter::possiblyContains(const string& elem) const {
	// Proveravamo da li je element u skupu
	// Ako je bar jedan bit postavljen na 0, onda element sigurno nije u skupu
	// Ako su svi bitovi postavljeni na 1, onda element mozda jeste u skupu
	// U tom slucaju vracamo true, ali postoji verovatnoca da je to lazno pozitivan rezultat

    for (const auto& hashFunction : hashFunctions) {
        size_t h = hashFunction(elem);
        if (!bitSet[h]) {
            return false;
        }
    }
    return true;
}

void BloomFilter::add(const string& elem) {
    for (const auto& hashFunction : hashFunctions) {
        bitSet[hashFunction(elem)] = true;
    }
}

vector<byte> BloomFilter::serialize() const {
    // Izracunamo koliko bajtova treba za bitSet
    size_t bitBytes = (m + 7) / 8;
    size_t totalSize = sizeof(m) + sizeof(k) + sizeof(p) + sizeof(timeConst) + sizeof(h2_seed) + bitBytes;

    vector<byte> data(totalSize);
    size_t offset = 0;

    // m
    memcpy(data.data() + offset, &m, sizeof(m));
    offset += sizeof(m);

    // k
    memcpy(data.data() + offset, &k, sizeof(k));
    offset += sizeof(k);

    // p
    memcpy(data.data() + offset, &p, sizeof(p));
    offset += sizeof(p);

    // timeConst
    memcpy(data.data() + offset, &timeConst, sizeof(timeConst));
    offset += sizeof(timeConst);

    // h2_seed
    memcpy(data.data() + offset, &h2_seed, sizeof(h2_seed));
    offset += sizeof(h2_seed);

	// Sada bitSet u bajtove
    for (size_t i = 0; i < bitBytes; i++) {
        uint8_t byteVal = 0;
        for (int b = 0; b < 8; b++) {
            size_t bitIndex = i * 8 + b;
            if (bitIndex < m && bitSet[bitIndex]) {
                byteVal |= (1 << b);
            }
        }
        data[offset++] = static_cast<byte>(byteVal);
    }

    return data;
}

BloomFilter BloomFilter::deserialize(const vector<byte>& data) {
    BloomFilter bf(1, 0.01); // Kreiramo privremeni, odmah cemo ga prepisati
    // Ovaj konstruktor nije bitan, jer cemo sve parametre prepisati iz data
    // a potom ponovo kreirati hashFunctions.

    size_t offset = 0;

    // m 
    memcpy(&bf.m, data.data() + offset, sizeof(bf.m));
    offset += sizeof(bf.m);

    // k
    memcpy(&bf.k, data.data() + offset, sizeof(bf.k));
    offset += sizeof(bf.k);

	// p
    memcpy(&bf.p, data.data() + offset, sizeof(bf.p));
    offset += sizeof(bf.p);

	// timeConst
    memcpy(&bf.timeConst, data.data() + offset, sizeof(bf.timeConst));
    offset += sizeof(bf.timeConst);

	// h2_seed
    memcpy(&bf.h2_seed, data.data() + offset, sizeof(bf.h2_seed));
    offset += sizeof(bf.h2_seed);

    // bitSet
    bf.bitSet = vector<bool>(bf.m, false);
    size_t bitBytes = (bf.m + 7) / 8;

    for (size_t i = 0; i < bitBytes; i++) {
        uint8_t byteVal = static_cast<uint8_t>(data[offset++]);
        for (int b = 0; b < 8; b++) {
            size_t bitIndex = i * 8 + b;
            if (bitIndex < bf.m && (byteVal & (1 << b)) != 0) {
                bf.bitSet[bitIndex] = true;
            }
        }
    }

	// Sada ponovo kreiramo hashFunctions
    bf.hashFunctions.clear();
    bf.hashFunctions.reserve(bf.k);

    unsigned int m_copy = bf.m;
    size_t seed_copy = bf.h2_seed;

    for (unsigned int i = 0; i < bf.k; ++i) {
        bf.hashFunctions.emplace_back(
            [i, m_copy, seed_copy](const string& key) {
                size_t h1 = hash<string>()(key);
                size_t h2 = hash<string>()(to_string(seed_copy) + key);
                return (h1 + i * h2) % m_copy;
            }
        );
    }

    return bf;
}

unsigned int BloomFilter::calculateSizeOfBitSet(unsigned int expectedElements, double falsePositiveRate) {
    return static_cast<unsigned int>(ceil(-static_cast<double>(expectedElements) * log(falsePositiveRate) / (log(2) * log(2))));
}

unsigned int BloomFilter::calculateNumberOfHashFunctions(unsigned int expectedElements, unsigned int m) {
    unsigned int k = static_cast<unsigned int>(round((m / static_cast<double>(expectedElements)) * log(2)));
    return (k == 0) ? 1 : k;
}
