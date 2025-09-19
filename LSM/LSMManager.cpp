#include "LSMManager.h"


struct HeapItem
{
    Record rec;
    int srcIdx; // indeks sstable iz koje je zapis dosao

    /*// Min-heap po kljucu (leksikografski redosled) => sortiranje po kljucu
    bool operator<(const HeapItem& other) const {
        return rec.key < other.rec.key;
    }*/
};

struct HeapItemMinHeapCmp {
    bool operator()(const HeapItem& a, const HeapItem& b) const {
        return a.rec.key > b.rec.key; // min-heap
    }
};

// Procitaj sledeci record iz sstalbe idx, offset cuvamo u offsets[idx] i gurnemo u heap
static bool readAndPushNext(
    int idx,
    SSTable* table,
    std::vector<uint64_t>& offsets,
    std::priority_queue<HeapItem, std::vector<HeapItem>, HeapItemMinHeapCmp >& heap)
{
    bool error = false;
    Record r = table->getNextRecord(offsets[idx], error);
    if (!error) {
        heap.push(HeapItem{ std::move(r), idx });
        return true;
    }
    return false; // Došli smo do kraja ove tabele (EOF)
}

// k-way merge svih SSTable-ova na nivou
// ulazi su pojedinacno sortirani po kljucu
// cuvamo onaj sa najvecim timestampom

static std::vector<Record> kWayMerge(const std::vector<SSTable*>& inputs)
{
    std::vector<Record> out;
    out.reserve(1024); // Pre-alokacija za performanse

    const int n = static_cast<int>(inputs.size());
    if (n == 0) return out;

    std::vector<uint64_t> offsets(n, 0);
    std::priority_queue<HeapItem, std::vector<HeapItem>, HeapItemMinHeapCmp> heap;

    // Učitaj prvi zapis iz svake ulazne SSTabele
    for (int i = 0; i < n; ++i) {
        readAndPushNext(i, inputs[i], offsets, heap);
    }

    while (!heap.empty()) {
        HeapItem first = heap.top();
        heap.pop();

        std::string currKey = first.rec.key;
        Record winner = first.rec; // Kandidat sa najvećim timestamp-om za trenutni ključ

        // Učitaj sledeći zapis iz tabele iz koje je došao "first"
        readAndPushNext(first.srcIdx, inputs[first.srcIdx], offsets, heap);

        // Obradi sve ostale zapise sa istim ključem koji su trenutno na vrhu heap-a
        while (!heap.empty() && heap.top().rec.key == currKey) {
            HeapItem same = heap.top();
            heap.pop();

            // Zadržavamo samo zapis sa najnovijim timestamp-om
            if (same.rec.timestamp > winner.timestamp) {
                winner = same.rec;
            }
            readAndPushNext(same.srcIdx, inputs[same.srcIdx], offsets, heap);
        }

        // Dodaj pobednika u izlaz (tombstone se čuva da bi se propagiralo brisanje)
        out.push_back(std::move(winner));
    }

    return out; // Vraća vektor zapisa sortiran po ključu
}

// Leveled: opseg kljuceva
struct KeyRange {
    std::string min_key;
    std::string max_key;
    bool empty = true;
};

// Izracunaj opseg kljuceva skeniranjem cele tabele
static KeyRange computeKeyRangeByScan(SSTable& t)
{
    uint64_t off = 0;
    bool err = false;
    KeyRange kr{};

    Record first = t.getNextRecord(off, err);
    if (err) return kr; // Prazna tabela

    kr.min_key = first.key;
    kr.max_key = first.key;
    kr.empty = false;

    // Skeniraj do kraja; poslednji zapis ima najveći ključ
    while (true) {
        Record r = t.getNextRecord(off, err);
        if (err) break;
        kr.max_key = r.key;
    }
    return kr;
}

// Proveri da li se dva opsega kljuceva preklapaju (ukljucujuci granicne vrednosti)
static inline bool overlapsInclusive(const KeyRange& a, const KeyRange& b)
{
    if (a.empty || b.empty) return false;
    return !(a.max_key < b.min_key || b.max_key < a.min_key);
}


// Leveled: Limiti po nivou
static long long fileLimitForLevel(int level, int l0_trigger, int multiplier)
{
    if (level <= 0) return std::max(1, l0_trigger);
    long long cap = std::max(1, l0_trigger);
    for (int i = 1; i <= level; ++i) {
        cap *= std::max(1, multiplier);
        if (cap > (1LL << 30)) break; // Zaštita od prelivanja
    }
    return cap;
}

// ------------------ Leveled: jedna kompakcija na nivou ------------------
// Iz nivoa L izabere 1 SSTable i spoji ga sa svim preklapajucim sa L+1
// Rezultat upise na L+1. Obrise BAS te ulaze. Vraca true ako je nesto uradjeno
static bool leveledCompactOneAtLevel(
    int level,
    SSTManager* sstManager)
{
    const int maxLevels = std::max(1, (int)Config::max_levels);
    if (level >= maxLevels - 1) return false; // Poslednji nivo se ne kompaktuje

    auto tables_L = sstManager->getTablesFromLevel(level);
    long long limit = fileLimitForLevel(level, Config::l0_compaction_trigger, Config::level_size_multiplier);

    if ((long long)tables_L.size() < limit) return false; // Nema potrebe za kompakcijom
    if (tables_L.empty()) return false;

    // Izaberi jednu tabelu sa nivoa L (ovde uzimamo prvu)
    auto chosen_table = std::move(tables_L.front());
    KeyRange chosenKR = computeKeyRangeByScan(*chosen_table);

    // Pronađi sve preklapajuće tabele na nivou L+1
    auto tables_L1 = sstManager->getTablesFromLevel(level + 1);
    std::vector<std::unique_ptr<SSTable>> overlapping_L1;
    std::vector<std::unique_ptr<SSTable>> non_overlapping_L1; // Ove ostaju na L+1

    for (auto& table : tables_L1) {
        KeyRange kr = computeKeyRangeByScan(*table);
        if (overlapsInclusive(chosenKR, kr)) {
            overlapping_L1.push_back(std::move(table));
        }
        else {
            non_overlapping_L1.push_back(std::move(table));
        }
    }

    // Kreiraj grupu za spajanje
    std::vector<SSTable*> merge_group_ptrs;
    merge_group_ptrs.push_back(chosen_table.get());
    for (const auto& table : overlapping_L1) {
        merge_group_ptrs.push_back(table.get());
    }

    // Spoji sve odabrane tabele
    std::vector<Record> merged = kWayMerge(merge_group_ptrs);

    // Upiši rezultat na nivo L+1
    if (!merged.empty()) {
        sstManager->write(merged, level + 1);
    }

    // Obriši ulazne tabele
    std::vector<std::unique_ptr<SSTable>> to_del_L;
    to_del_L.push_back(std::move(chosen_table));
    sstManager->removeSSTables(level, to_del_L);
    sstManager->removeSSTables(level + 1, overlapping_L1);

    return true;
}
// Kompaktuje ceo nivo L u jednu SSTabelu na nivou L+1 i briše stari nivo L.
// Vraća true ako je izvršena kompakcija.
static bool compactFullLevelOnce(
    int level,
    SSTManager* sstManager,
    int threshold,
    int maxLevels)
{
    if (level >= maxLevels - 1) return false;

    auto tables = sstManager->getTablesFromLevel(level);
    if (static_cast<int>(tables.size()) < threshold) return false;

    // Kreiraj grupu pokazivača za spajanje
    std::vector<SSTable*> group_ptrs;
    group_ptrs.reserve(tables.size());
    for (const auto& table : tables) {
        group_ptrs.push_back(table.get());
    }

    // Spoji sve tabele sa nivoa
    std::vector<Record> merged = kWayMerge(group_ptrs);

    // Upiši rezultat na sledeći nivo
    if (!merged.empty()) {
        sstManager->write(merged, level + 1);
    }

    // Obriši sve stare SSTabele sa nivoa L
    sstManager->removeSSTables(level, tables);

    return true;
}

LSMManager::LSMManager(SSTManager* sstManager)
    : sstManager(sstManager)
{
    assert(this->sstManager != nullptr && "SSTManager must not be null");
}

LSMManager::~LSMManager() = default;

void LSMManager::triggerCompactionCheck()
{
    if (Config::compaction_strategy == "leveled")
    {
        const int maxLevels = std::max(1, (int)Config::max_levels);
        bool any_compaction;
        do {
            any_compaction = false;
            // Prolazi od nivoa 0 nagore
            for (int L = 0; L < maxLevels - 1; ++L) {
                bool compacted_at_level;
                do {
                    compacted_at_level = leveledCompactOneAtLevel(L, sstManager);
                    if (compacted_at_level) {
                        any_compaction = true;
                    }
                } while (compacted_at_level);
            }
        } while (any_compaction); // Ponavljaj dok god ima promena
    }
    else // SIZE-TIERED (i podrazumevana)
    {
        const int maxLevels = std::max(1, static_cast<int>(Config::max_levels));
        const int threshold = std::max(2, static_cast<int>(Config::max_number_of_sstable_on_level));

        bool any_compaction;
        do {
            any_compaction = false;
            // Prolazi od nivoa 0 nagore
            for (int level = 0; level < maxLevels - 1; ++level) {
                // Petlja se izvršava dokle god je nivo prenatrpan
                // jer jedna kompakcija može osloboditi mesto, ali nivo i dalje može biti iznad praga
                bool compacted_at_level;
                do {
                    compacted_at_level = compactFullLevelOnce(level, sstManager, threshold, maxLevels);
                    if (compacted_at_level) {
                        any_compaction = true;
                    }
                } while (compacted_at_level);
            }
        } while (any_compaction); // Ponavljaj ceo proces ako je bilo koja kompakcija pokrenula kaskadnu
    }
}
