#pragma once

#include <string>
#include <optional>
#include "IMemtable.h"

using namespace std;

template<int ORDER> class BTree : public IMemtable
{
    struct BTreeNode
    {
        string keys[ORDER-1];
        string values[ORDER-1];
        
        BTreeNode* children[ORDER];

        int numKeys;

        ~BTreeNode()
        {
            delete[] children;
        }

        BTreeNode() : numKeys(0)
        {
            for(int i = 0; i < ORDER; i++)
            {
                children[i] = nullptr;
            }
        }

        bool isLeaf()
        {
            return children[0] == nullptr;
        }
    };

    ~BTree() override
    {
        delete root;
    }

    void splitChild(BTreeNode* parent, BTreeNode* child, int childPos);

    void insert(const std::string& key, const std::string& value);
    void insertNotFull(BTreeNode* x, const string& key, const string& value);
    
    //location je in-out
    BTreeNode* findNode(BTreeNode* start, const string& key, int &location) const;

    void removeInner(BTreeNode* x, const int pos);
    void removeOuter(BTreeNode* x, const int pos);

    //val je in-out
    string findPred(BTreeNode* x, int pos, string& val);
    string findSuccessor(BTreeNode* x, int pos, string& val);

    void mergeChild(BTreeNode* parent, int childPos);
    void redistribute(BTreeNode* parent, int leftChildPos);

    BTreeNode* root;
    size_t entryCount;
    size_t maxSize;
    
public:
    BTree(size_t maxSize_) : entryCount(0), maxSize(maxSize_) { root = new BTreeNode(); }

    void flush();
    
    void put(const std::string& key, const std::string& value) override;

    void remove(const std::string& key) override;
    bool remove(BTreeNode* x, const std::string& key);

    optional<std::string> get(const std::string& key) const override;

    size_t size() const override;

    void setMaxSize(size_t maxSize) override;

    void loadFromWal(const std::string& wal_file) override;
    
};
