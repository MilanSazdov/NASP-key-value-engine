#include "../SSTable/SSTable.h"
#include "../SSTable/SSTableRaw.h"
#include "../SSTable/SSTableComp.h"
#include <memory>

class SSTableIteratorException : public std::exception {
public:
    const char* what() const noexcept override {
        return "Reading outside SSTable.";
    }
};


class SSTableIterator {
public:
    SSTableIterator(std::shared_ptr<SSTable> sst);

    Record operator*();
    void operator++();
    void seek(uint64_t offset);
    
    bool reached_end;
    uint64_t offset;

    
private:
    Record cached;
    bool cache_valid;

    std::shared_ptr<SSTable> sstp;
    uint64_t next_offset;

};