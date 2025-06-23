#include "simhash.h"
#include "../MurmurHash3/MurmurHash3.h"
#include <stdexcept>

using namespace std;

SimHash::SimHash() {
    const vector<string> stopWordsList = {
        // English
        "I", "ME", "MY", "MYSELF", "WE", "OUR", "OURS", "OURSELVES", "YOU", "YOUR", "YOURS", "YOURSELF", "YOURSELVES", "HE", "HIM",
        "HIS", "HIMSELF", "SHE", "HER", "HERS", "HERSELF", "IT", "ITS", "ITSELF", "THEY", "THEM", "THEIR", "THEIRS", "THEMSELVES", "WHAT",
        "WHICH", "WHO", "WHOM", "THIS", "THAT", "THESE", "THOSE", "AM", "IS", "ARE", "WAS", "WERE", "BE", "BEEN", "BEING",
        "HAVE", "HAS", "HAD", "HAVING", "DO", "DOES", "DID", "DOING", "A", "AN", "THE", "AND", "BUT", "IF", "OR",
        "BECAUSE", "AS", "UNTIL", "WHILE", "OF", "AT", "BY", "FOR", "WITH", "ABOUT", "AGAINST", "BETWEEN", "INTO", "THROUGH", "DURING",
        "BEFORE", "AFTER", "ABOVE", "BELOW", "TO", "FROM", "UP", "DOWN", "IN", "OUT", "ON", "OFF", "OVER", "UNDER", "AGAIN",
        "FURTHER", "THEN", "ONCE", "HERE", "THERE", "WHEN", "WHERE", "WHY", "HOW", "ALL", "ANY", "BOTH", "EACH", "FEW", "MORE",
        "MOST", "OTHER", "SOME", "SUCH", "NO", "NOR", "NOT", "ONLY", "OWN", "SAME", "SO", "THAN", "TOO", "VERY", "S",
        "T", "CAN", "WILL", "JUST", "DON", "SHOULD", "NOW",
        // Serbian (with utf8 characters)
        "I", "U", "JE", "SE", "DA", "SU", "NA", "SA", "ZA", "OD", "DO", "KAO", "STO", "STA",
        "ALI", "ILI", "PAK", "CAK", "SVE", "BIO", "BIH", "BITI", "NEGO", "KADA", "KOJI",
        "KOJA", "KOJE", "ONI", "BUDE", "BUDU", "JEDAN", "JEDNA", "JEDNO", "NEKI", "NEKA",
        "NEKO", "NJIH", "CIJI", "CIJA", "CIJE", "MOJ", "MOJA", "MOJE", "TVOJ", "TVOJA",
        "TVOJE", "NAS", "NASA", "NASE", "VAM", "VAMA", "NAMA", "ONO", "ONI", "ONA",
        "NISAM", "NISI", "NIJE", "NISMO", "NISTE", "NISU", "JESAM", "JESI", "JESMO",
        "JESTE", "JESU"
    };
    for (const string& word : stopWordsList) {
        stopWords[word] = true;
    }
}

vector<string> SimHash::parseText(const string& text) const {
    vector<string> words;
    string currentWord;

    for (char c : text) {
        if (isalnum(c)) {
            currentWord += toupper(c);
        }
        else if (!currentWord.empty()) {
            if (!stopWords.count(currentWord)) {
                words.push_back(currentWord);
            }
            currentWord.clear();
        }
    }

    if (!currentWord.empty() && !stopWords.count(currentWord)) {
        words.push_back(currentWord);
    }

    return words;
}

uint64_t SimHash::hashWord(const string& word) const {
    uint64_t hash[2] = { 0 };
    MurmurHash3_x64_128(word.c_str(), word.length(), 0, hash);
    return hash[0];
}

uint64_t SimHash::computeFingerprint(const vector<string>& words) const {
    vector<int> counts(64, 0);

    for (const string& word : words) {
        uint64_t hash = hashWord(word);
        for (int i = 0; i < 64; ++i) {
            if (hash & (1ULL << i)) {
                counts[i]++;
            }
            else {
                counts[i]--;
            }
        }
    }

    uint64_t fingerprint = 0;
    for (int i = 0; i < 64; ++i) {
        if (counts[i] > 0) {
            fingerprint |= (1ULL << i);
        }
    }

    return fingerprint;
}


uint64_t SimHash::getFingerprint(const string& text) const {
    vector<string> words = parseText(text);
    return computeFingerprint(words);
}

int SimHash::hammingDistance(const string& text1, const string& text2) const {
    uint64_t fp1 = getFingerprint(text1);
    uint64_t fp2 = getFingerprint(text2);
    
    uint64_t xor_result = fp1 ^ fp2;
    
    int distance = 0;
    while (xor_result) {
        distance += xor_result & 1;
        xor_result >>= 1;
    }
    
    return distance;
}