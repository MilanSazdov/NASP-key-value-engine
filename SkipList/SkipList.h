#pragma once

#include <string>
#include <vector>
#include <random>
#include <memory>
#include <optional>

using namespace std;

class SkipList {

private:
    struct Node {

        string key;
        string value;
        bool tombstone;
		uint64_t timestamp; // vreme poslednje izmene
        // forward[i] ukazuje na cvor u nivou i
        vector<Node*> forward;

        Node(const std::string& k, const std::string& v, bool t, uint64_t ts, int level)
            : key(k), value(v), tombstone(t), timestamp(ts), forward(level, nullptr) {}
    };

    // Generise nasumicni nivo za novi cvor
    int generateRandomLevel();

    // trazimo cvorove prethodnike za svaki nivo pre umetanja, odnsno brisanja
    void findPrevNodes(const string& key, vector<Node*>& update) const;

    unsigned int maxLevels;
    double p;
    int currentLevel; // trenutni najvisi nivo popunjen
    size_t size;
    Node* head; // glava - sentinel cvor
    mt19937_64 rng;
    uniform_real_distribution<double> dist;


public:
    // Konstruktor prima maksimalan broj nivoa i vjerovatnocu za visinu
    // novih cvorova. maxLevels = maksimalni nivoi, p = vjerovatnoca povecanja nivoa (obicno 0.5)
    SkipList(int maxLevels = 16, double p = 0.5);

    ~SkipList();

    // Umece ili azurira (key, value)
    void insert(const string& key, const string& value, bool tombstone, uint64_t timestamp);

    // Brise cvor s datim kljucem (ako postoji)
    void remove(const string& key);

    // Pronalazenje value po key. Ako ne postoji, vraca NULL
    optional<string> get(const string& key) const;

	// Vraca cvor s datim kljucem (ako postoji)
	Node* getNode(const string& key) const;

    // Vraca velicinu (broj elemenata)
    size_t Size() const { return size; }

	vector<pair<string, string>> getAllKeyValuePairs() const;
};

