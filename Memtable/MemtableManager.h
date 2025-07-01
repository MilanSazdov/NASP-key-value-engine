#pragma once

#include <vector>
#include <memory>
#include <string>
#include <optional>
#include "IMemtable.h"
#include "SSTManager.h"
#include "LSMManager.h"

class MemtableManager {
public:
    /**
     * @param type tip Memtable ("hash", "skiplist", "btree")
     * @param N maksimalan broj memtable instanci u memoriji
     * @param maxSizePerTable koliko elemenata moze stati u svaku memtable
     * @param directory direktorijum - ako je relative, mora "./", i mora da se zavrsava sa /. Ako se izostavi, default je "./".
     **/
    MemtableManager(const std::string& type,
        size_t N,
        size_t maxSizePerTable,
        const std::string& directory
    );

    ~MemtableManager();

    // Upis kljuca i vrednosti u aktivnu memtable
    void put(const std::string& key, const std::string& value);

    void remove(const std::string& key);

    // Dohvatanje vrednosti iz memtable (po potrebi i iz sstable)
    std::optional<std::string> get(const std::string& key) const;

    // Kad zelimo rucno flush (npr. gasenje programa),
    // ili ako se popune sve N memtables
    void flushAll();

    // Kada se sistem pokrene, Memtable treba popuniti zapisima iz WAL-a
    void loadFromWal(const std::vector<Record>& records);

	// Print all data from memtables
	void printAllData() const;

private:
    std::string type_;   // sacuvamo koji tip je korisnik izabrao
    size_t N_;           // max broj memtable
    size_t maxSize_;     // max broj elemenata u svakoj
    std::string directory_;

    std::unique_ptr<SSTManager> sstManager_;
    std::unique_ptr<LSMManager> lsmManager_;

    // N instanci memtable
    std::vector<std::unique_ptr<IMemtable>> memtables_;

    // Indeks "aktivne" (read-write) memtable
    size_t activeIndex_ = 0;

    // Pomocna: kreira novu memtable (koristeci MemtableFactory)
    IMemtable* createNewMemtable() const;

    // Ako se aktivna memtable popuni, prelazimo na novu
    void switchToNewMemtable();

    void flushOldest(); // prazni samo najstariju memtable (prvu napravljenu)

    void checkAndFlushIfNeeded(); // proverava da li je potrebno preci na novu tabelu ili uraditi flush
};

