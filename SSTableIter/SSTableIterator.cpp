#include "SSTableIterator.h"

static constexpr uint64_t NO_OFFSET = UINT64_MAX;

SSTableIterator::SSTableIterator(std::shared_ptr<SSTable> sst) : sstp(sst), next_offset(NO_OFFSET) {
    seek(sst->getDataStartOffset());
};

void SSTableIterator::operator++(){
    if(next_offset == NO_OFFSET) {
        bool error = false;
        sstp->getNextRecord(offset, error);
        if(error) {
            reached_end = true;
        }

        return;
    }

    offset = next_offset;
    next_offset = NO_OFFSET;
}

Record SSTableIterator::operator*(){
    uint64_t temp_offset = offset;
    bool error = false;

    Record r = sstp->getNextRecord(offset, error);

    if(error) throw new SSTableEndException();

    next_offset = offset;
    offset = temp_offset;

    return r;
}

void SSTableIterator::seek(uint64_t offset){
    this->offset = offset;
    next_offset = NO_OFFSET;
}

