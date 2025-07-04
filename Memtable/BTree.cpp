#include "BTree.h"

#include <fstream>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <vector>
#include <cstring>
#include <ctime>

template <int ORDER>
void BTree<ORDER>::splitChild(BTreeNode* parent, BTreeNode* child, int childPos)
{
    BTreeNode* newNode = new BTreeNode();
    newNode->numKeys = ORDER / 2 - 1;

    //Kopiramo poslednjih t-1 kljuceva i poslednje t dece (2*t = order)
    const int t = ORDER / 2;
    for (int i = 0; i < t - 1; ++i)
    {
        newNode->keys[i] = child->keys[i + t];
        newNode->entries[i] = child->entries[i + t];
    }

    if (!child->isLeaf())
    {
        for (int i = 0; i < t; ++i)
            newNode->children[i] = child->children[i + t];
    }

    child->numKeys = t - 1;

    //Pravimo mesta za novi node
    for (int i = parent->numKeys; i >= childPos + 1; --i)
        parent->children[i + 1] = parent->children[i];

    parent->children[childPos + 1] = newNode;

    //Pravimo mesta za novi kljuc
    for (int i = parent->numKeys - 1; i >= childPos; --i)
    {
        parent->keys[i + 1] = parent->keys[i];
        parent->entries[i + 1] = parent->entries[i];
    }

    parent->keys[childPos] = child->keys[t - 1];
    parent->entries[childPos] = child->entries[t - 1];
    parent->numKeys = parent->numKeys + 1;
}

template <int ORDER>
void BTree<ORDER>::insert(const std::string& key, const Entry& entry)
{
    //Root je pun, splitujemo ga
    if (root->numKeys == ORDER - 1)
    {
        BTreeNode* z = new BTreeNode();
        z->children[0] = root;

        splitChild(z, root, 0);

        if (z->keys[0] > key)
            insertNotFull(z->children[0], key, entry);
        else
            insertNotFull(z->children[1], key, entry);

        root = z;
    }
    else
        insertNotFull(root, key, entry);

}


template <int ORDER>
void BTree<ORDER>::insertNotFull(BTreeNode* x, const std::string& key, const Entry& entry)
{
    while (!x->isLeaf())
    {
        BTreeNode* y;
        int i;
        if (key > x->keys[x->numKeys - 1])
        {
            y = x->children[x->numKeys];
            i = x->numKeys;
        }
        else
        {
            i = 0;
            while (i < x->numKeys && key > x->keys[i])
            {
                ++i;
            }
            y = x->children[i];
        }
        //Provera da li je y pun
        if (y->numKeys == ORDER - 1)
        {
            //y je pun, preemptivno delimo
            this->splitChild(x, y, i);

            if (key < x->keys[i])
            {
                x = x->children[i];
            }
            else
            {
                x = x->children[i + 1];
            }
        }
        else
        {
            x = y;
        }
    }

    //Splitovali smo sve pune cvorove do sada, tako da znamo da x nije pun. U njega stavljamo kljuc
    int i = x->numKeys - 1;
    while (i >= 0 && x->keys[i] > key)
    {
        x->keys[i + 1] = x->keys[i];
        x->entries[i + 1] = x->entries[i];
        --i;
    }

    x->keys[i + 1] = key;
    x->entries[i + 1] = entry;
    x->numKeys += 1;
}

// OVDE MOZE BINARNA PRETRAGA => IPAK JE BINARNO STABLO U PITANJU
template <int ORDER>
typename BTree<ORDER>::BTreeNode* BTree<ORDER>::findNode(BTreeNode* start, const std::string& key, int& location) const
{
    int i = 0;

    
    while (i < start->numKeys && key > start->keys[i])
    {
        ++i;
    }

    if (i < start->numKeys && key == start->keys[i])
    {
        location = i;
        return start;
    }

    if (start->isLeaf())
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
    Entry e = { value, false , currentTime() };
    put(key, e);
}

template <int ORDER>
void BTree<ORDER>::put(const std::string& key, const Entry& entry) {
    int location;
    BTreeNode* n = findNode(root, key, location);

    //Ne postoji kljuc, insertujemo
    if (n == nullptr)
    {
        if (entryCount == maxSize)
        {
            cerr << "[BTree] B-Tree je pun.";
            return;
        }

        insert(key, entry);
        ++entryCount;
    }
    else
    {
        //Update
        n->entries[location] = entry;
    }
}

template <int ORDER>
void BTree<ORDER>::remove(const std::string& key)
{
    if (remove(root, key))
    {
        --entryCount;
        // Obrisali smo entry, dodajemo novi sa tombstone 1 da bi obrisali i one u SSTable.
        // (Kada remove(BTreeNode, string) vrati false, on svakako dodaje novi entry u funkciji.)
        Entry e = { "", true, currentTime() };
        put(key, e);
    }
}

template <int ORDER>
bool BTree<ORDER>::remove(BTreeNode* x, const std::string& key)
{
    int i = 0;
    while (i < x->numKeys && key > x->keys[i])
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

    Entry e = { "", true, currentTime() };
    put(key, e);
    return false;
}

template <int ORDER>
void BTree<ORDER>::removeInner(BTreeNode* x, const int pos)
{
    if (x->children[pos]->numKeys >= ORDER / 2)
    {
        Entry e;
        std::string predKey = findPred(x, pos, e);
        x->keys[pos] = predKey;
        x->entries[pos] = e;
        remove(x->children[pos], predKey);
    }

    else if (x->children[pos + 1]->numKeys >= ORDER / 2)
    {
        Entry e;
        std::string succesorKey = findSuccessor(x, pos, e);
        x->keys[pos] = succesorKey;
        x->entries[pos] = e;
        remove(x->children[pos + 1], succesorKey);
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
    for (int i = pos + 1; i < x->numKeys; ++i)
    {
        x->keys[i - 1] = x->keys[i];
        x->entries[i - 1] = x->entries[i];
    }


    x->numKeys -= 1;
}

template <int ORDER>
std::string BTree<ORDER>::findPred(BTreeNode* x, int pos, Entry& e)
{
    BTreeNode* y = x->children[pos];
    while (!y->isLeaf())
    {
        y = y->children[y->numKeys];
    }

    e = y->entries[y->numKeys - 1];
    return y->keys[y->numKeys - 1];
}

template <int ORDER>
std::string BTree<ORDER>::findSuccessor(BTreeNode* x, int pos, Entry& e)
{
    BTreeNode* y = x->children[x->numKeys];
    while (!y->isLeaf())
    {
        y = y->children[0];
    }

    e = y->entries[0];
    return y->keys[0];
}

template <int ORDER>
void BTree<ORDER>::mergeChild(BTreeNode* parent, int childPos)
{
    BTreeNode* leftChild = parent->children[childPos];
    BTreeNode* rightChild = parent->children[childPos + 1];

    int leftNumKeys = leftChild->numKeys;
    leftChild->keys[leftNumKeys] = parent->keys[childPos];
    leftChild->entries[leftNumKeys] = parent->entries[childPos];

    for (int i = childPos; i < parent->numKeys - 1; ++i)
    {
        parent->keys[i] = parent->keys[i + 1];
        parent->entries[i] = parent->entries[i + 1];
        parent->children[i + 1] = parent->children[i + 2];
    }

    parent->numKeys -= 1;

    for (int i = 0; i < rightChild->numKeys; ++i)
    {
        leftChild->keys[leftNumKeys + 1 + i] = rightChild->keys[i];
        leftChild->entries[leftNumKeys + 1 + i] = rightChild->entries[i];
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
            rightChild->entries[i] = rightChild->entries[i - 1];
        }

        if (!rightChild->isLeaf())
        {
            for (int i = rightChild->numKeys + 1; i > 0; --i)
                rightChild->children[i] = rightChild->children[i - 1];
        }

        rightChild->keys[0] = parent->keys[leftChildPos];
        rightChild->entries[0] = parent->entries[leftChildPos];

        if (!leftChild->isLeaf())
            rightChild->children[0] = leftChild->children[leftChild->numKeys];

        parent->keys[leftChildPos] = leftChild->keys[leftChild->numKeys - 1];
        parent->entries[leftChildPos] = leftChild->entries[leftChild->numKeys - 1];

        leftChild->numKeys -= 1;
        rightChild->numKeys += 1;
    }
    else
    {
        // Borrow iz desnog
        leftChild->keys[leftChild->numKeys] = parent->keys[leftChildPos];
        leftChild->entries[leftChild->numKeys] = parent->entries[leftChildPos];

        if (!rightChild->isLeaf())
            leftChild->children[leftChild->numKeys + 1] = rightChild->children[0];

        parent->keys[leftChildPos] = rightChild->keys[0];
        parent->entries[leftChildPos] = rightChild->entries[0];

        for (int i = 0; i < rightChild->numKeys - 1; ++i)
        {
            rightChild->keys[i] = rightChild->keys[i + 1];
            rightChild->entries[i] = rightChild->entries[i + 1];
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

    if (n == nullptr) return std::nullopt;

    if (n->entries[location].tombstone == true) return std::nullopt;

    return n->entries[location].value;
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
std::vector<MemtableEntry> BTree<ORDER>::getAllMemtableEntries() const
{
    std::vector<MemtableEntry> result;
    inorder(root, result);
    return result;
}

template <int ORDER>
void BTree<ORDER>::inorder(BTreeNode* node, std::vector<MemtableEntry>& entries) const
{
    if (!node) return;

    for (int i = 0; i < node->numKeys; ++i)
    {
        if (!node->isLeaf())
            inorder(node->children[i], entries);

        MemtableEntry e;
        e.key = node->keys[i];
        e.timestamp = node->entries[i].timestamp;
        e.value = node->entries[i].value;
        e.tombstone = node->entries[i].tombstone;
        entries.emplace_back(e);
    }

    // Najdesniji cvor
    if (!node->isLeaf())
        inorder(node->children[node->numKeys], entries);
}

template <int ORDER>
std::optional<MemtableEntry> BTree<ORDER>::getEntry(const std::string& key) const {
    int location;
    BTreeNode* node = findNode(root, key, location);

    if (node == nullptr) {
        return std::nullopt; // Ako ključ ne postoji, vraćamo std::nullopt
    }

    const Entry& entry = node->entries[location];

    // Vraćamo MemtableEntry sa informacijama o ključu
    return MemtableEntry{
        key,
        entry.value,
        entry.tombstone,
        entry.timestamp
    };
}

template <int ORDER>
void BTree<ORDER>::updateEntry(const std::string& key, const MemtableEntry& entry) {
    int location;
    BTreeNode* node = findNode(root, key, location);

    if (node == nullptr) {
        // Ako ključ ne postoji, ispisujemo grešku
        std::cerr << "[BTree] Key '" << key << "' not found for update.\n";
        return;
    }

    // Ažuriramo vrednosti u postojećem čvoru
    node->entries[location].value = entry.value;
    node->entries[location].tombstone = entry.tombstone;
    node->entries[location].timestamp = entry.timestamp;

    std::cout << "[BTree] Key '" << key << "' updated successfully.\n";
}