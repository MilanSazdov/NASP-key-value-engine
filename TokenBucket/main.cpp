#include "TokenBucket.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>

using namespace std;

uint64_t currentTimeSec() {
    return chrono::duration_cast<chrono::seconds>(chrono::system_clock::now().time_since_epoch()).count();
}

void testSerialization(const TokenBucket& bucket) {
    vector<byte> data = bucket.serialize();

    TokenBucket deserialized = TokenBucket::deserialize(data);

    cout << "\nTest nakon serijalizacije i deserijalizacije:\n";
    cout << "Max tokena: " << deserialized.getMaxTokens() << endl;
    cout << "Interval punjenja (sekunde): " << deserialized.getRefillIntervalSec() << endl;
    cout << "Preostali tokeni: " << deserialized.getTokens() << endl;
}

int main() {
    uint32_t maxTokens = 5;
    uint64_t refillInterval = 10; // puni se na svakih 10 sekundi

    TokenBucket bucket(maxTokens, refillInterval);

    cout << "Testiranje Token Bucket-a:\n";
    cout << "Dozvoljeno je ukupno " << maxTokens << " zahteva u periodu od " << refillInterval << " sekundi.\n";

    uint64_t startTime = currentTimeSec();

    // Pokusaj 7 zahteva odmah
    for (int i = 1; i <= 7; ++i) {
        uint64_t now = currentTimeSec() - startTime;
        bool allowed = bucket.allowRequest();
        cout << "Vreme: " << now << "s | Zahtev " << i << ": " << (allowed ? "Dozvoljen" : "Odbijen") << " | Preostali tokeni: " << bucket.getTokens() << endl;
        this_thread::sleep_for(chrono::milliseconds(500)); // mala pauza radi preglednosti
    }

    // Cekaj da se bucket napuni
    cout << "\nCekanje " << refillInterval + 1 << " sekundi...\n";
    this_thread::sleep_for(chrono::seconds(refillInterval + 1));

    // Ponovni zahtevi
    for (int i = 1; i <= 3; ++i) {
        uint64_t now = currentTimeSec() - startTime;
        bool allowed = bucket.allowRequest();
        cout << "Vreme: " << now << "s | Zahtev " << i << ": " << (allowed ? "Dozvoljen" : "Odbijen") << " | Preostali tokeni: " << bucket.getTokens() << endl;
    }

    testSerialization(bucket);

    return 0;
}
