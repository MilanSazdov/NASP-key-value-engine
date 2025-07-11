#pragma once

#include <string>
#include <vector>
#include <filesystem>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>

#include "SSTManager.h"
#include "wal.h"
#include "Compaction.h"

// deklaracija unapred da se izbegnu ciklicne zavisnosti
class CompactionStrategy;

class LSMManager {
public:
    // konstruktor sada prima odabranu strategiju i maksimalan broj nivoa
    LSMManager(std::unique_ptr<CompactionStrategy> strategy, int max_levels, SSTManager& sstRef);

    // destruktor se brine o bezbednom zaustavljanju pozadinske niti
    ~LSMManager();

    // inicijalizuje foldere i pokrece pozadinsku nit
    void initialize();

    // metoda koju poziva MemtableManager nakon sto se Memtable napuni
    void flushToLevel0(std::vector<Record>& records);

    std::optional<std::string> get(const std::string& key);

private:
    // glavna funkcija za pozadinsku nit
    void compaction_worker();

    // metoda koja izvrsava konkretan plan kompakcije dobijen od strategije
    void do_compaction(const CompactionPlan& plan);

    std::string getLevelPath(int level) const;

	void load_state_from_manifest();
	void save_state_to_manifest() const; // const jer ne menja stanje samo ga cuva

    std::string base_directory_;
	std::string manifest_path_; // putanja do MANIFEST fajla
    int max_levels_;
    SSTManager& sstManager_;
    std::unique_ptr<CompactionStrategy> strategy_;

    // Centralna struktura koja cuva stanje LSM stabla u memoriji
    std::vector<std::vector<SSTableMetadata>> levels_;

    // Mutex za zastitu pristupa `levels_` strukturi iz vise niti
    std::mutex mtx_;

    // Komponente za upravljanje i sinhronizaciju pozadinske niti
    std::thread compaction_thread_;
    std::condition_variable cv_;
    std::atomic<bool> stop_worker_;
    std::atomic<bool> compaction_needed_; // Signal da nit treba da proveri da li ima posla
};

