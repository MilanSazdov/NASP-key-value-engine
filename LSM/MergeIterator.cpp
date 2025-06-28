#include "MergeIterator.h"

MergeIterator::MergeIterator(std::vector<std::unique_ptr<SSTableIterator>> iterators)
    : iterators_(std::move(iterators)) {
    // inicijalizuj heap sa prvim elementom iz svakog validnog iteratora
    for (size_t i = 0; i < iterators_.size(); ++i) {
        if (iterators_[i] && iterators_[i]->hasNext()) {
            min_heap_.push({ iterators_[i]->next(), i });
        }
    }
    // odmah pronadji prvi validan zapis.
    findNextValid();
}

bool MergeIterator::hasNext() const {
    return next_record_.has_value();
}

Record MergeIterator::next() {
    if (!hasNext()) {
        throw std::out_of_range("No more elements in MergeIterator");
    }
    //vrati pre-pripremljeni zapis.
    Record current = std::move(next_record_.value());
    // odmah pripremi sledeci validan zapis.
    findNextValid();
    return current;
}

void MergeIterator::findNextValid() {
    next_record_.reset(); // Ocisti bafer

    while (!min_heap_.empty()) {
        // uzmi globalno najmanji (i najnoviji) element sa vrha heap-a
        HeapElement top = min_heap_.top();
        min_heap_.pop();

        // odmah povuci sledeci element iz istog izvora i stavi ga u heap
        if (iterators_[top.source_index]->hasNext()) {
            min_heap_.push({ iterators_[top.source_index]->next(), top.source_index });
        }

        // logika za preskakanje starih verzija istog kljuca
        // ako smo vec obradili ovaj kljuc, znaci da je ovo starija verzija, pa je preskacemo.
        if (has_processed_first_key_ && top.record.key == last_key_processed_) {
            continue;
        }

        // zapamti poslednji obradjeni kljuc.
        last_key_processed_ = top.record.key;
        has_processed_first_key_ = true;

        // ako je najnovija verzija Tombstone, preskoci je i sve starije verzije
        // ne zelimo da se obrisani zapisi pojave u novom SSTable-u
        if (static_cast<int>(top.record.tombstone) == 1) {
            continue;
        }

        // nasli smo validan, najnoviji zapis koji nije obrisan
        // sacuvaj ga u bafer i prekini petlju
        next_record_ = top.record;
        return;
    }
}