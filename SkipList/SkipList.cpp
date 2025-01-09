#include "SkipList.h"
#include <limits>
#include <iostream>


SkipList::SkipList(int maxLevels, double p)
    : maxLevels(maxLevels),
    p(p),
    currentLevel(0),
    size(0),
    rng(std::random_device{}()),
    dist(0.0, 1.0)
{
    // Kreiramo head cvor (sentinel) sa maksimalnim nivoima
    // On nece imati valjan key/value, sluzi kao pocetna tacka
    head = new Node("", "", false, 0, maxLevels);
}


SkipList::~SkipList() {
    Node* current = head;
    while (current != nullptr) {
        Node* next = current->forward[0];
        delete current;
        current = next;
    }
}

int SkipList::generateRandomLevel() {
    unsigned int level = 1;
    while (dist(rng) < p && level < maxLevels) {
        level++;
    }
    return level;
}

void SkipList::findPrevNodes(const string& key, vector<Node*>& update) const {
    // krecemo od head-a i trazimo mesto gde ce key biti umetnut
    Node* current = head;
    for (int i = currentLevel; i >= 0; i--) {
        // idemo napred dok ne pronadjemo key
        while (current->forward[i] != nullptr && current->forward[i]->key < key) {
            current = current->forward[i];
        }
        update[i] = current;
    }
}

void SkipList::insert(const std::string& key, const std::string& value, bool tombstone, uint64_t timestamp) {
    // update vektor za pointere prethodnika
    std::vector<Node*> update(maxLevels, nullptr);
    findPrevNodes(key, update);

    Node* next = update[0]->forward[0];
    // Ako postoji cvor s tim kljucem, samo update value
    if (next != nullptr && next->key == key) {
        next->value = value;
        next->tombstone = tombstone;
        next->timestamp = timestamp;
        return;
    }

    // Ako ne postoji, moramo ugraditi novi cvor
    int newLevel = generateRandomLevel();
    if (newLevel > currentLevel) {
        for (int i = currentLevel + 1; i < newLevel; i++) {
            update[i] = head;
        }
        currentLevel = newLevel - 1;
    }

    Node* newNode = new Node(key, value, tombstone, timestamp, newLevel);
    for (int i = 0; i < newLevel; i++) {
        newNode->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = newNode;
    }
    size++;
}

void SkipList::remove(const string& key) {
    vector<Node*> update(maxLevels, nullptr);
    findPrevNodes(key, update);

    Node* target = update[0]->forward[0];
    if (target == nullptr || target->key != key) {
        // ne postoji takav key, nema brisanja
        return;
    }

    // Podesiti linkove da preskoce target
    for (int i = 0; i <= currentLevel; i++) {
        if (update[i]->forward[i] != target)
            break;
        update[i]->forward[i] = target->forward[i];
    }

    delete target;
    size--;

    // Smanjiti currentLevel_ ako je potrebno
    while (currentLevel > 0 && head->forward[currentLevel] == nullptr) {
        currentLevel--;
    }
}

std::optional<std::string> SkipList::get(const string& key) const {
    Node* current = head;
    for (int i = currentLevel; i >= 0; i--) {
        while (current->forward[i] != nullptr && current->forward[i]->key < key) {
            current = current->forward[i];
        }
    }
    current = current->forward[0];
    if (current != nullptr && current->key == key) {
        return current->value;
    }
    return std::nullopt;
}

SkipList::Node* SkipList::getNode(const std::string& key) const {
    Node* current = head;
    for (int i = currentLevel; i >= 0; i--) {
        while (current->forward[i] != nullptr && current->forward[i]->key < key) {
            current = current->forward[i];
        }
    }
    current = current->forward[0];
    if (current != nullptr && current->key == key) {
        return current;
    }
    return nullptr;
}

vector<pair<string, string>> SkipList::getAllKeyValuePairs() const {
	vector<pair<string, string>> result;
	Node* current = head->forward[0];
	while (current != nullptr) {
		result.push_back({ current->key, current->value });
		current = current->forward[0];
	}
	return result;
}

