#pragma once

#include <vector>
#include <queue>
#include <memory>
#include "SSTable.h"
#include "SSTableIterator.h"

// element koji se cuva u min-heapu
struct HeapElement {

    Record record;
    size_t source_index; // indeks iteratora iz kojeg potice zapis

    // komparator za min-heap. prvo poredi kljuceve, a ako su isti,
    // prednost daje zapisu iz novijeg izvora (manji source_index)

    bool operator>(const HeapElement& other) const {
        if (record.key != other.record.key) {
            return record.key > other.record.key;
        }
        // ako su kljucevi isti, noviji zapis (sa manjim indeksom) ima veci prioritet (manju vrednost)
        return source_index > other.source_index;
    }
};

class MergeIterator {
public:
    // konstruktor prima vektore SSTable iteratora. Iteratori moraju biti poredjani
    // od najnovijih ka najstarijim izvorima da bi se duplikati ispravno resavali.
    MergeIterator(std::vector<std::unique_ptr<SSTableIterator>> iterators);

    // vraca true ako ima još elemenata
    bool hasNext() const;

    // vraca sledeci, spojen i filtriran zapis
    Record next();

private:

    std::vector<std::unique_ptr<SSTableIterator>> iterators_;
    std::priority_queue<HeapElement, std::vector<HeapElement>, std::greater<HeapElement>> min_heap_;

    // bafer za sledeci zapis koji ce biti vracen
    std::optional<Record> next_record_;

    // koristi se za pracenje i preskakanje starijih verzija istog kljuca
    std::string last_key_processed_;
    bool has_processed_first_key_ = false;

    // pronalazenje sledeceg validnog zapisa.
    void findNextValid();
};
