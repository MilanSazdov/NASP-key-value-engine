#include "BTree.h"

#include <fstream>
#include <optional>
#include <stdexcept>
#include <iostream>

using namespace std;

template <int ORDER>
void BTree<ORDER>::splitChild(BTreeNode* parent, BTreeNode* child, int childPos)
{
    BTreeNode* newNode = new BTreeNode();
    newNode->numKeys = ORDER/2-1;

    //Kopiramo poslednjih t-1 kljuceva i poslednje t dece (2*t = order)
    const int t = ORDER/2;
    for (int i = 0; i < t-1; ++i)
    {
        newNode->keys[i] = child->keys[i+t];
        newNode->values[i] = child->values[i+t];
    }

    if (!child->isLeaf())
    {
        for (int i = 0; i < t; ++i)
            newNode->children[i] = child->children[i+t];
    }

    child->numKeys = t-1;

    //Pravimo mesta za novi node
    for (int i = parent->numKeys; i >= childPos+1; --i)
        parent->children[i+1] = parent->children[i];

    parent->children[childPos+1] = newNode;

    //Pravimo mesta za novi kljuc
    for (int i = parent->numKeys-1; i >= childPos; --i)
    {
        parent->keys[i+1] = parent->keys[i];
        parent->values[i+1] = parent->values[i];
    }

    parent->keys[childPos] = child->keys[t-1];
    parent->values[childPos] = child->values[t-1];
    parent->numKeys = parent->numKeys + 1;
}

template <int ORDER>
void BTree<ORDER>::insert(const std::string& key, const std::string& value)
{
    //Root je pun, splitujemo ga
    if (root->numKeys == ORDER-1)
    {
        BTreeNode *z = new BTreeNode();
        z->children[0] = root;

        splitChild(z, root, 0);
        
        if (z->keys[0] > key)
            insertNotFull(z->children[0], key, value);
        else
            insertNotFull(z->children[1], key, value);
        
        root = z;
    }
    else
        insertNotFull(root, key, value);
    
}


template <int ORDER>
void BTree<ORDER>::insertNotFull(BTreeNode* x, const string& key, const string& value)
{
    while(!x->isLeaf())
    {
        BTreeNode* y;
        int i;
        if (key > x->keys[x->numKeys-1])
        {
            y = x->children[x->numKeys];
            i = x->numKeys;
        } else
        {
            i = 0;
            while (i < x->numKeys && key > x->keys[i])
            {
                ++i;
            }
            y = x->children[i];
        }
        //Provera da li je y pun
        if(y->numKeys == ORDER-1)
        {
            //y je pun, preemptivno delimo
            this->splitChild(x, y, i);

            if (key < x->keys[i])
            {
                x = x->children[i];
            } else
            {
                x = x->children[i+1];
            }
        } else
        {
            x = y;
        }
    }

    //Splitovali smo sve pune cvorove do sada, tako da znamo da x nije pun. U njega stavljamo kljuc
    int i = x->numKeys-1;
    while (i >= 0 && x->keys[i] > key)
    {
        x->keys[i+1] = x->keys[i];
        x->values[i+1] = x->values[i];
        --i;
    }

    x->keys[i+1] = key;
    x->values[i+1] = value;
    x->numKeys += 1;
}

template <int ORDER>
typename BTree<ORDER>::BTreeNode* BTree<ORDER>::findNode(BTreeNode* start, const string& key, int &location) const
{
    int i = 0;

    while(key > start->keys[i] && i < start->numKeys)
    {
        ++i;
    }

    if(key == start->keys[i] && i < start->numKeys)
    {
        location = i;
        return start;
    }
    
    if(start->isLeaf())
        return nullptr;

    return findNode(start->children[i], key, location);
}

template <int ORDER>
void BTree<ORDER>::flush()
{
    throw std::logic_error("Nema");
}

template <int ORDER>
void BTree<ORDER>::put(const std::string& key, const std::string& value)
{
    if(entryCount == maxSize)
    {
        cerr<<"[BTree] B-Tree je pun.";
        return;
    }
    
    int location;
    BTreeNode* n = findNode(root, key, location);

    //Ne postoji kljuc, insertujemo
    if(n == nullptr)
    {
        insert(key, value);
        ++entryCount;
    }
    else
        n->values[location] = value;
}

template <int ORDER>
void BTree<ORDER>::remove(const std::string& key)
{
    if(remove(root, key))
    {
        --entryCount;
    }
    
}

template <int ORDER>
bool BTree<ORDER>::remove(BTreeNode* x, const std::string& key)
{
    int i = 0;
    while(i < x->numKeys && key > x->keys[i])
        ++i;

    if (i < x->numKeys && x->keys[i] == key)
    {
        if (x->isLeaf())
            removeOuter(x, i);
        else
            removeInner(x, i);

        return true;
    }

    if (!x->isLeaf()) 
        {
        BTreeNode* child = x->children[i];
        if (child->numKeys < ORDER / 2) 
            {
            if (i > 0 && x->children[i - 1]->numKeys >= ORDER / 2)
                redistribute(x, i - 1); // Borrow iz levog
            else if (i < x->numKeys && x->children[i + 1]->numKeys >= ORDER / 2)
                redistribute(x, i); // Borrow iz desnog
            else
            {
                if (i < x->numKeys)
                    mergeChild(x, i); // Merge sa desnim
                else
                    mergeChild(x, i - 1); // Merge sa levim
                child = x->children[i > 0 ? i - 1 : i];
            }
            }
        return remove(child, key);
        }

    return false;
}

template <int ORDER>
void BTree<ORDER>::removeInner(BTreeNode* x, const int pos)
{
    if (x->children[pos]->numKeys >= ORDER/2)
    {
        string val;
        string predKey = findPred(x, pos, val);
        x->keys[pos] = predKey;
        x->values[pos] = val;
        remove(x->children[pos], predKey);
    }
    
    else if  (x->children[pos+1]->numKeys >= ORDER/2)
    {
        string val;
        string succesorKey = findSuccessor(x, pos, val);
        x->keys[pos] = succesorKey;
        x->values[pos] = val;
        remove(x->children[pos+1], succesorKey);
    }
    
    else
    {
        mergeChild(x, pos);
        remove(x->children[pos], x->keys[pos]);
    }
}

template <int ORDER>
void BTree<ORDER>::removeOuter(BTreeNode* x, const int pos)
{
    for (int i = pos+1; i<x->numKeys; ++i)
    {
        x->keys[i-1] = x->keys[i];
        x->values[i-1] = x->values[i];
    }


    x->numKeys -= 1;
}

template <int ORDER>
string BTree<ORDER>::findPred(BTreeNode* x, int pos, string& val)
{
    BTreeNode* y = x->children[pos];
    while(!y->isLeaf())
    {
        y = y->children[y->numKeys];
    }

    val = y->values[y->numKeys-1];
    return y->keys[y->numKeys-1];
}

template <int ORDER>
string BTree<ORDER>::findSuccessor(BTreeNode* x, int pos, string& val)
{
    BTreeNode* y = x->children[x->numKeys];
    while(!y->isLeaf())
    {
        y = y->children[0];
    }

    val = y->values[0];
    return y->keys[0];
}

template <int ORDER>
void BTree<ORDER>::mergeChild(BTreeNode* parent, int childPos)
{
    BTreeNode* leftChild = parent->children[childPos];
    BTreeNode* rightChild = parent->children[childPos + 1];

    int leftNumKeys = leftChild->numKeys;
    leftChild->keys[leftNumKeys] = parent->keys[childPos];
    leftChild->values[leftNumKeys] = parent->values[childPos];

    for (int i = childPos; i < parent->numKeys - 1; ++i)
    {
        parent->keys[i] = parent->keys[i + 1];
        parent->values[i] = parent->values[i + 1];
        parent->children[i + 1] = parent->children[i + 2];
    }

    parent->numKeys -= 1;

    for (int i = 0; i < rightChild->numKeys; ++i)
    {
        leftChild->keys[leftNumKeys + 1 + i] = rightChild->keys[i];
        leftChild->values[leftNumKeys + 1 + i] = rightChild->values[i];
    }

    if (!leftChild->isLeaf())
    {
        for (int i = 0; i <= rightChild->numKeys; ++i)
        {
            leftChild->children[leftNumKeys + 1 + i] = rightChild->children[i];
        }
    }

    leftChild->numKeys += 1 + rightChild->numKeys;

    delete rightChild;
}


//Funkcija gleda dvoje susedne dece, gde bar jedan ima vise od t kljuceva, a drugi manje
template <int ORDER>
void BTree<ORDER>::redistribute(BTreeNode* parent, int leftChildPos)
{
    BTreeNode* leftChild = parent->children[leftChildPos];
    BTreeNode* rightChild = parent->children[leftChildPos + 1];

    if (leftChild->numKeys >= ORDER / 2)
    {
        // Borrow iz levog deteta
        for (int i = rightChild->numKeys; i > 0; --i)
        {
            rightChild->keys[i] = rightChild->keys[i - 1];
            rightChild->values[i] = rightChild->values[i - 1];
        }

        if (!rightChild->isLeaf())
        {
            for (int i = rightChild->numKeys + 1; i > 0; --i)
                rightChild->children[i] = rightChild->children[i - 1];
        }

        rightChild->keys[0] = parent->keys[leftChildPos];
        rightChild->values[0] = parent->values[leftChildPos];

        if (!leftChild->isLeaf())
            rightChild->children[0] = leftChild->children[leftChild->numKeys];

        parent->keys[leftChildPos] = leftChild->keys[leftChild->numKeys - 1];
        parent->values[leftChildPos] = leftChild->values[leftChild->numKeys - 1];

        leftChild->numKeys -= 1;
        rightChild->numKeys += 1;
    }
    else
    {
        // Borrow iz desnog
        leftChild->keys[leftChild->numKeys] = parent->keys[leftChildPos];
        leftChild->values[leftChild->numKeys] = parent->values[leftChildPos];

        if (!rightChild->isLeaf())
            leftChild->children[leftChild->numKeys + 1] = rightChild->children[0];

        parent->keys[leftChildPos] = rightChild->keys[0];
        parent->values[leftChildPos] = rightChild->values[0];

        for (int i = 0; i < rightChild->numKeys - 1; ++i)
        {
            rightChild->keys[i] = rightChild->keys[i + 1];
            rightChild->values[i] = rightChild->values[i + 1];
        }

        if (!rightChild->isLeaf())
        {
            for (int i = 0; i < rightChild->numKeys; ++i)
                rightChild->children[i] = rightChild->children[i + 1];
        }

        leftChild->numKeys += 1;
        rightChild->numKeys -= 1;
    }
}

template <int ORDER>
optional<std::string> BTree<ORDER>::get(const std::string& key) const
{
    int location;
    BTreeNode* n = findNode(root, key, location);

    if(n==nullptr) return std::nullopt;

    return n->values[location];
}

template <int ORDER>
size_t BTree<ORDER>::size() const
{
    return entryCount;
}

template <int ORDER>
void BTree<ORDER>::setMaxSize(size_t newMaxSize)
{
    maxSize = newMaxSize;
}

template <int ORDER>
void BTree<ORDER>::loadFromWal(const std::string& wal_file)
{
    std::ifstream file(wal_file);
    if (!file.is_open()) {
        std::cerr << "[BTree] Ne mogu da otvorim WAL fajl: " << wal_file << "\n";
        return;
    }

    std::string op, key, value;
    while (file >> op >> key) {
        if (op == "INSERT") {
            file >> value;
            put(key, value);
        }
        else if (op == "REMOVE") {
            remove(key);
        }
    }
    file.close();
}

