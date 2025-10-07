#include "simhash.h"
#include <iostream>
#include <iomanip>

using namespace std;

void printFingerprint(const uint64_t fingerprint) {
    cout << "Fingerprint: ";
    for (int i = 63; i >= 0; i--) {
        cout << ((fingerprint >> i) & 1);
    }
    cout << endl;
}

int main() {
    SimHash simHash;

    // Test 1: Prikaz fingerprinta za jednostavan tekst
    string simpleText = "Hello World from Mars";
    cout << "\nTest 1 - Simple text fingerprint:" << endl;
    cout << "Text: " << simpleText << endl;
    auto simpleFingerprint = simHash.getFingerprint(simpleText);
    auto parseWords = simHash.parseText(simpleText);
    for (auto word : parseWords) {
        cout << word << endl;
    }
    printFingerprint(simpleFingerprint);

    // Test 2: Identicni tekstovi
    string text1 = "The quick brown fox jumps over the lazy dog";
    string text2 = "The quick brown fox jumps over the lazy dog";
    auto fingerprint1 = simHash.getFingerprint(text1);
    auto fingerprint2 = simHash.getFingerprint(text2);
    int distance1 = simHash.hammingDistance(text1, text2);

    cout << "\nTest 2 - Identical texts:" << endl;
    cout << "Text 1 fingerprint: ";
    printFingerprint(fingerprint1);
    cout << "Text 2 fingerprint: ";
    printFingerprint(fingerprint2);
    cout << "Distance: " << distance1 << endl;

    // Test 3: Veliki tekst 
    string bigText1 = "From Wikipedia, the free encyclopedia"
        "In computer science, SimHash is a technique for quickly estimating how similar two sets are.The algorithm is used by the Google Crawler to find near duplicate pages.It was created by Moses Charikar.In 2021 Google announced its intent to also use the algorithm in their newly created FLoC(Federated Learning of Cohorts) system.[1]"
        "Evaluation and benchmarks"
        "A large scale evaluation has been conducted by Google in 2006[2] to compare the performance of Minhash and Simhash[3] algorithms.In 2007 Google reported using Simhash for duplicate detection for web crawling[4] and using Minhash and LSH for Google News personalization.[5";

    string bigText2 = "Wikipedia, free encyclopedia"
        "In computer science, SimHash technique for quickly estimating how similar two sets are.The algorithm is used by the Google Crawler to find near duplicate pages.It was created by Moses Charikar.In 2021 Google announced its intent to also use the algorithm in their newly created FLoC(Federated Learning of Cohorts) system.[1]"
        "A large scale evaluation has been conducted by Google in 2006[2] to compare the performance of Minhash and Simhash[3] algorithms.In 2007 Google reported using Simhash for duplicate detection for web crawling[4] and using Minhash and LSH for Google News personalization.[5";

    cout << "\nTest 3 - Large texts:" << endl;
    cout << "Big Text 1 length: " << bigText1.length() << " characters" << endl;
    cout << "Big Text 2 length: " << bigText2.length() << " characters" << endl;
    auto bigFingerprint1 = simHash.getFingerprint(bigText1);
    auto bigFingerprint2 = simHash.getFingerprint(bigText2);
    cout << "Text 1 fingerprint: ";
    printFingerprint(bigFingerprint1);
    cout << "Text 2 fingerprint: ";
    printFingerprint(bigFingerprint2);
    cout << "Distance: " << simHash.hammingDistance(bigText1, bigText2) << endl;

    // Test 4: Tekst sa specijalnim karakterima i ponavljanjima
    string text7 = "Hello, hello... HELLO!!! World, world, WORLD!!!";
    string text8 = "Hello World";
    cout << "\nTest 4 - Text with special chars and repetitions:" << endl;
    auto fingerprint7 = simHash.getFingerprint(text7);
    auto fingerprint8 = simHash.getFingerprint(text8);
    cout << "Text 1 fingerprint: ";
    printFingerprint(fingerprint7);
    cout << "Text 2 fingerprint: ";
    printFingerprint(fingerprint8);
    cout << "Distance: " << simHash.hammingDistance(text7, text8) << endl;

    // Test 5: Srpski tekst (latinica)
    string text9 = "Brzi mrki lisac skace preko lenjog psa, ali pas ne reaguje i ostaje na svom mestu, lenj kao i obicno.";
    string text10 = "Brzi mrki lisac skoci preko lenjog psa, dok se vetar igra sa liscem koje polako pada na zemlju.";
    cout << "\nTest 5 - Serbian text (Latin):" << endl;
    auto fingerprint9 = simHash.getFingerprint(text9);
    auto fingerprint10 = simHash.getFingerprint(text10);
    cout << "Text 1 fingerprint: ";
    printFingerprint(fingerprint9);
    cout << "Text 2 fingerprint: ";
    printFingerprint(fingerprint10);
    cout << "Distance: " << simHash.hammingDistance(text9, text10) << endl;

    return 0;
}