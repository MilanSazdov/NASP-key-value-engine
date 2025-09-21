#include "../SSTable/SSTable.h"
#include "../SSTable/SSTableRaw.h"
#include "../SSTable/SSTableComp.h"
#include <memory>

class SSTableEndException : public std::exception {
public:
    char* what() {
        return "Reached end of SSTable.";
    }
};


class SSTableIterator {
public:
    // SSTableIterator(const std::string& dataFile,
    //                 const std::string& indexFile,
    //                 const std::string& filterFile,
    //                 const std::string& summaryFile,
    //                 const std::string& metaFile,
    //                 Block_manager* bmp);

    // SSTableIterator(const std::string& dataFile,
    //                 Block_manager* bmp);
    
    // SSTableIterator(const std::string& dataFile,
    //             const std::string& indexFile,
    //             const std::string& filterFile,
    //             const std::string& summaryFile,
    //             const std::string& metaFile,
    //             Block_manager* bmp,
    //             unordered_map<string, uint32_t>& key_to_id,
    //             vector<string>& id_to_key,
    //             uint32_t& nextId);

    // SSTableIterator(const std::string& dataFile,
    //             Block_manager* bmp,
    //             unordered_map<string, uint32_t>& key_to_id,
    //             vector<string>& id_to_key,
    //             uint32_t& nextId);

    SSTableIterator(std::shared_ptr<SSTable> sst);

    Record operator*();
    void operator++();
    void seek(uint64_t offset);
    
    bool reached_end;
    uint64_t offset;
    
private:
    std::shared_ptr<SSTable> sstp;
    uint64_t next_offset;

};