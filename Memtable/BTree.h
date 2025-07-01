#pragma once

#include <string>
#include <optional>
#include "IMemtable.h"
#include <vector>
#include <cstdint>
#include <chrono>
#include <memory>
#include <iostream>
#include <fstream>
#include <stdexcept>

template<int ORDER>
class BTree : public IMemtable
{
    struct Entry {
        std::string value;
        bool tombstone;
        uint64_t timestamp;
    };

    struct BTreeNode
    {

        std::string keys[ORDER - 1];
        Entry entries[ORDER - 1];

        BTreeNode* children[ORDER];

        int numKeys;

        ~BTreeNode()
        {
            for (int i = 0; i < ORDER; i++)
            {
                delete children[i]; // Delete each child node if it exists
                children[i] = nullptr; // Optional: Nullify pointer for safety
            }
        }

        BTreeNode() : numKeys(0)
        {
            for (int i = 0; i < ORDER; i++)
            {
                children[i] = nullptr;
            }
        }

        bool isLeaf()
        {
            return children[0] == nullptr;
        }
    };

    BTreeNode* root;
    size_t entryCount;
    size_t maxSize;


    void inorder(BTreeNode* node, std::vector<MemtableEntry>& entries) const;

    void splitChild(BTreeNode* parent, BTreeNode* child, int childPos);

    void insert(const std::string& key, const Entry& entry);
    void insertNotFull(BTreeNode* x, const std::string& key, const Entry& entry);

    //location je in-out
    BTreeNode* findNode(BTreeNode* start, const std::string& key, int& location) const;

    void removeInner(BTreeNode* x, const int pos);
    void removeOuter(BTreeNode* x, const int pos);

    //val je in-out
    std::string findPred(BTreeNode* x, int pos, Entry& e);
    std::string findSuccessor(BTreeNode* x, int pos, Entry& e);

    void mergeChild(BTreeNode* parent, int childPos);
    void redistribute(BTreeNode* parent, int leftChildPos);

    static uint64_t currentTime() {
        return static_cast<uint64_t>(
            std::chrono::system_clock::now().time_since_epoch().count()
            );
    }

public:
    ~BTree() override
    {
        delete root;
    }

    BTree(size_t maxSize_) : entryCount(0), maxSize(maxSize_) { root = new BTreeNode(); }

    void flush();

    void put(const std::string& key, const std::string& value) override;
    void put(const std::string& key, const Entry& entry);

    void remove(const std::string& key) override;
    bool remove(BTreeNode* x, const std::string& key);

    optional<std::string> get(const std::string& key) const override;

    size_t size() const override;

    void setMaxSize(size_t maxSize) override;

    void loadFromWal(const std::string& wal_file) override;

    //std::vector<pair<string, string>> getAllKeyValuePairs() const override

    std::vector<MemtableEntry> getAllMemtableEntries() const override;

	std::optional<MemtableEntry> getEntry(const std::string& key) const override;

	void updateEntry(const std::string& key, const MemtableEntry& entry) override;

    virtual std::vector<MemtableEntry> getSortedEntries() const override;
};
