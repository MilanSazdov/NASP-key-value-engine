#include "SSTableIter.h"

static constexpr uint64_t NO_OFFSET = UINT64_MAX;

SSTableIterator::SSTableIterator(std::shared_ptr<SSTable> sst) : sstp(sst), next_offset(NO_OFFSET), cache_valid(false) {
    seek(sst->getDataStartOffset());
};

void SSTableIterator::operator++(){
    cache_valid = false;
    bool error = false;
    bool eof = false;
    sstp->getNextRecord(offset, error, eof);
    if(eof) {
        reached_end = true;
    }
    if(error) {
        throw SSTableIteratorException();
    }
    return;
}

Record SSTableIterator::operator*(){
    if (cache_valid) return cached;

    uint64_t temp_offset = offset;
    bool error = false;
    bool eof = false;

    Record r = sstp->getNextRecord(offset, error, eof);

    if(error) throw SSTableIteratorException();

    offset = temp_offset;

    cached = r;
    cache_valid = true;

    return r;
}

void SSTableIterator::seek(uint64_t offset){
    this->offset = offset;
    next_offset = NO_OFFSET;
}

