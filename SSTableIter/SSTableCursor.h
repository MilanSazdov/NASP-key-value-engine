#include "../SSTable/SSTManager.h"
#include "../Memtable/MemtableManager.h"
#include "SSTableIter.h"
#include <list>

class SSTableCursor {
    struct Candidate{
        shared_ptr<SSTable> sstp;
        SSTableIterator sst_iter;
        Record curr_record;

        Candidate(const std::shared_ptr<SSTable>& sstp_,
                const SSTableIterator& sst_iter_,
                Record curr_record_)
      : sstp(sstp_), sst_iter(sst_iter_), curr_record(curr_record_)
    {}
    };

public:   
    SSTableCursor(SSTManager* sstmp, MemtableManager* mtmp);

    // Fetches `page_size` number of records with prefix `prefix` from SSTables
    std::vector<Record> prefix_scan(const std::string& prefix, int page_size, bool& end);
    vector<Record> range_scan(const std::string& min_key, const std::string& max_key, int page_size, bool& end);
    void reset();

    void read_tables();
    
private:
    SSTManager* sst_manager;
    MemtableManager* memt_manager;
    vector<shared_ptr<SSTable>> sstables;
    list<Candidate> candidates;
    vector<SSTableIterator> sst_iterators;

    vector<MemtableEntry> memtableCandidates;

    void prepare_prefix_scan(const std::string& prefix);
    void prepare_range_scan(const std::string& min_key,const std::string& max_key);


    bool range_scan_prepared;
    bool prefix_scan_prepared;
};