#include "SSTableCursor.h"
#include <filesystem>
#include <regex>

namespace fs = std::filesystem;

SSTableCursor::SSTableCursor(SSTManager* sstmp,
                             MemtableManager* mtmp)
                            : sst_manager(sstmp),
                              memt_manager(mtmp),
                              prefix_scan_prepared(false),
                              range_scan_prepared(false)
{
    read_tables();
}


void SSTableCursor::read_tables() {
    const fs::path data_dir = Config::data_directory;
    const std::regex re("^level_(\\d+)$");
    std::vector<int> levels;

    if (fs::exists(data_dir) && fs::is_directory(data_dir)) {
        for (auto const& entry : fs::directory_iterator(data_dir)) {

            // Proveravamo da li je direktorijum
            if (!fs::is_directory(entry.status())) continue;

            std::string name = entry.path().filename().string();
            std::smatch m;
            if (std::regex_match(name, m, re)) {
                try {
                    int lvl = std::stoi(m[1].str());
                    levels.push_back(lvl);
                } catch (const std::exception& e) {
                    // Ignorisi
                }
            }
        }
    }

    for(int l : levels) {
        vector<unique_ptr<SSTable>> sst_from_level = sst_manager->getTablesFromLevel(l);

        sstables.reserve(sstables.size() + sst_from_level.size());

        for (auto &uptr : sst_from_level) {
            std::shared_ptr<SSTable> sptr = std::shared_ptr<SSTable>(std::move(uptr));
            sstables.push_back(sptr);
            // candidates.emplace_back(sptr);
            SSTableIterator ssti(sptr); // Pravimo novi iterator. Pocinje sa offset = data_start.
            sst_iterators.push_back(ssti);
        }
    }
}

vector<Record> SSTableCursor::prefix_scan(const std::string& prefix, int page_size, bool& end) {
    prepare_prefix_scan(prefix);

    vector<Record> ret;
    ret.reserve(page_size);
    
    if(candidates.empty() && memtableCandidates.empty()){
        end = true;
        return ret;
    }


    while(!candidates.empty()) {
        std::string min_key(256, char(0xFF));

        vector<Candidate*> min_candidates;
        // min_candidates.push_back(&(candidates.front()));
        // Candidate& min_candidate = candidates.front();

        vector<list<Candidate>::iterator> min_iterators;
        // min_iterators.push_back(candidates.begin());
        // auto min_iterator = candidates.begin();

        for (auto it = candidates.begin(); it != candidates.end();) { // Trazimo min kljuc
            std::string key = (*it).curr_record.key;
            if(key.rfind(prefix, 0) != 0) { // Ako kljuc ne pocinje sa prefiksom, ne gledamo vise sstabelu
                it = candidates.erase(it);
                continue;
            }

            if (key < min_key) {
                min_key = key;
                min_candidates.clear();
                min_candidates.push_back(&(*it));
                
                min_iterators.clear();
                min_iterators.push_back(it); // TODO: proveri da li kasnije ++it pomera min_iterator
                // min_iterator = it;
            }

            else if (key == min_key) {
                Candidate& candidate = *it;
                SSTableIterator& cand_iter = candidate.sst_iter;

                while(true) {
                    uint64_t saved_offset = cand_iter.offset;                  
                    
                    ++cand_iter; // Gledamo sledeci rekord
                    if(cand_iter.reached_end) {
                        cand_iter.seek(saved_offset); // Ako ne, vracamo se
                        break;
                    };

                    if((*cand_iter).key == candidate.curr_record.key) { // Ako sledeci rekord ima isti kljuc
                        candidate.curr_record = candidate.curr_record.timestamp > (*cand_iter).timestamp ? // Uzimamo najnoviji
                                                candidate.curr_record : (*cand_iter); 
                    } else {
                        cand_iter.seek(saved_offset); // Ako ne, vracamo se
                        break;
                    };
                }

                min_candidates.push_back(&candidate);
                min_iterators.push_back(it);
            }

            ++it;
        }

        // Imamo listu kandidata koji svi imaju isti, najmanji kljuc.

        // Da li u memtabeli ima manji?
        if(!memtableCandidates.empty()) {
            if(memtableCandidates.back().key < min_key) {
                Record r;
                r.key = memtableCandidates.back().key;
                r.value = memtableCandidates.back().value;
                r.timestamp = memtableCandidates.back().timestamp;
                r.tombstone = static_cast<std::byte>(memtableCandidates.back().tombstone);
                ret.push_back(r);
                memtableCandidates.pop_back();
                
                if(ret.size() == page_size && (!candidates.empty() || memtableCandidates.empty())) {
                    end = false;
                    return ret;
                }
    
                if(candidates.empty() && memtableCandidates.empty()){
                    end = true;
                    return ret;
                }
    
                continue;
            }
        }

        uint64_t newest_ts = min_candidates[0]->curr_record.timestamp;
        int newest_idx = 0;
        for(int i = 0; i < min_candidates.size(); ++i) {
            if (min_candidates[i]->curr_record.timestamp > newest_ts) {
                newest_ts = min_candidates[i]->curr_record.timestamp;
                newest_idx = i; 
            }
        }

        // Dodajemo najnoviji rekord sa minimalnim kljucem
        Record winner = min_candidates[newest_idx]->curr_record;

        // Ako u memtabeli isti kljuc, skidamo i njega.
        if(!memtableCandidates.empty()) {
            if(memtableCandidates.back().key == winner.key) {
                // Ako je u memtabeli noviji zapis
                if(memtableCandidates.back().timestamp > winner.timestamp) {
                    winner.value = memtableCandidates.back().value;
                    winner.timestamp = memtableCandidates.back().timestamp;
                    winner.tombstone = static_cast<byte>(memtableCandidates.back().tombstone);
                }
                memtableCandidates.pop_back();
            }
        }

        bool tombstone = static_cast<bool>(winner.tombstone);
        if(!tombstone) ret.push_back(winner);

        // Pomeramo sve iteratore za kandidate koji su imali min kljuc
        for(int i = 0; i < min_candidates.size(); ++i) {
            if (min_candidates[i]->sst_iter.reached_end) {
                candidates.erase(min_iterators[i]);
                continue;
            }

            ++(min_candidates[i]->sst_iter);
            if (!min_candidates[i]->sst_iter.reached_end) {
                min_candidates[i]->curr_record = *(min_candidates[i]->sst_iter);
                if(min_candidates[i]->curr_record.key.rfind(prefix, 0) != 0) {
                    candidates.erase(min_iterators[i]);
                }
            }
            else candidates.erase(min_iterators[i]);
            
        }
 
        if(ret.size() == page_size && (!candidates.empty() || memtableCandidates.empty())) {
            end = false;
            return ret;
        }

        if(candidates.empty() && memtableCandidates.empty()){
            end = true;
            return ret;
        } 
    }

    size_t needed = page_size - ret.size();
    size_t take = min(needed, memtableCandidates.size());

    for(int i = 0; i < take; ++i) {
        auto memEntry = memtableCandidates.back();
        Record r;
        r.key = memEntry.key;
        r.value = memEntry.value;
        r.timestamp = memEntry.timestamp;
        ret.push_back(r);
        memtableCandidates.pop_back();
    }

    end = memtableCandidates.empty();

    return ret;
}

vector<Record> SSTableCursor::range_scan(const std::string& min_key, const std::string& max_key, int page_size, bool& end) {
    prepare_range_scan(min_key, max_key);

    vector<Record> ret;
    ret.reserve(page_size);
    
    if(candidates.empty() && memtableCandidates.empty()){
        end = true;
        return ret;
    }

    while(!candidates.empty()) {
        // std::string min_key_sst = candidates.front().curr_record.key;
        std::string min_key_sst(256, char(0xFF));

        vector<Candidate*> min_candidates;
        // min_candidates.push_back(&(candidates.front()));
        // Candidate& min_candidate = candidates.front();

        vector<list<Candidate>::iterator> min_iterators;
        // min_iterators.push_back(candidates.begin());
        // auto min_iterator = candidates.begin();

        for (auto it = candidates.begin(); it != candidates.end();) { // Trazimo min kljuc
            std::string key = (*it).curr_record.key;
            if(key > max_key) { // Ako kljuc veci od maks, izasli smo iz opsega
                it = candidates.erase(it);
                continue;
            }

            if(key < min_key_sst) {
                min_key_sst = key;
                min_candidates.clear();
                min_candidates.push_back(&(*it));
                
                min_iterators.clear();
                min_iterators.push_back(it); // TODO: proveri da li kasnije ++it pomera min_iterator
            }

            else if (key == min_key_sst) {
                Candidate& candidate = *it;
                SSTableIterator& cand_iter = candidate.sst_iter;

                while(true) { // Citamo sve rekorde sa istim kljucem, trazimo najnoviji
                    uint64_t saved_offset = cand_iter.offset;                  
                    
                    ++cand_iter; // Gledamo sledeci rekord
                    if(cand_iter.reached_end){
                        cand_iter.seek(saved_offset);
                        break;
                    };

                    if((*cand_iter).key == candidate.curr_record.key) { // Ako sledeci rekord ima isti kljuc
                        candidate.curr_record = candidate.curr_record.timestamp > (*cand_iter).timestamp ? // Uzimamo najnoviji
                                                candidate.curr_record : (*cand_iter); 
                    } else {
                        cand_iter.seek(saved_offset); // Ako ne, vracamo se
                        break;
                    };
                }

                min_candidates.push_back(&candidate);
                min_iterators.push_back(it);
            }

            ++it;
        }


        // Imamo listu kandidata koji svi imaju isti, najmanji kljuc.

        // Da li u memtabeli ima manji?
        if(!memtableCandidates.empty()) {
            if(memtableCandidates.back().key < min_key_sst) {
                Record r;
                r.key = memtableCandidates.back().key;
                r.value = memtableCandidates.back().value;
                r.timestamp = memtableCandidates.back().timestamp;
                ret.push_back(r);
                memtableCandidates.pop_back();
                
                if(ret.size() == page_size && (!candidates.empty() || memtableCandidates.empty())) {
                    end = false;
                    return ret;
                }
    
                if(candidates.empty() && memtableCandidates.empty()){
                    end = true;
                    return ret;
                }
    
                continue;
            }
        }

        uint64_t newest_ts = min_candidates[0]->curr_record.timestamp;
        int newest_idx = 0;
        for(int i = 0; i < min_candidates.size(); ++i) {
            if (min_candidates[i]->curr_record.timestamp > newest_ts) {
                newest_ts = min_candidates[i]->curr_record.timestamp;
                newest_idx = i; 
            }
        }

        // Dodajemo najnoviji rekord sa minimalnim kljucem

        Record winner = min_candidates[newest_idx]->curr_record;

        // Ako u memtabeli isti kljuc, skidamo i njega.
        if(!memtableCandidates.empty()) {
            if(memtableCandidates.back().key == winner.key) {
                // Ako je u memtabeli noviji zapis
                if(memtableCandidates.back().timestamp > winner.timestamp) {
                    winner.value = memtableCandidates.back().value;
                    winner.timestamp = memtableCandidates.back().timestamp;
                    winner.tombstone = static_cast<byte>(memtableCandidates.back().tombstone);
                }
                memtableCandidates.pop_back();
            }
        }

        bool tombstone = static_cast<bool>(winner.tombstone);
        if(!tombstone) ret.push_back(winner);


        // Pomeramo sve iteratore za kandidate koji su imali min kljuc
        for(int i = 0; i < min_candidates.size(); ++i) {
            if (min_candidates[i]->sst_iter.reached_end) {
                candidates.erase(min_iterators[i]);
                continue;
            }


            ++(min_candidates[i]->sst_iter);
            if (!min_candidates[i]->sst_iter.reached_end) {
                min_candidates[i]->curr_record = *(min_candidates[i]->sst_iter);
                if(min_candidates[i]->curr_record.key > max_key) {
                    candidates.erase(min_iterators[i]);
                }
            }
            else candidates.erase(min_iterators[i]);
        }

        if(ret.size() == page_size && (!candidates.empty() || memtableCandidates.empty())) {
            end = false;
            return ret;
        }

        if(candidates.empty() && memtableCandidates.empty()){
            end = true;
            return ret;
        }
    }

    size_t needed = page_size - ret.size();
    size_t take = min(needed, memtableCandidates.size());

    for(int i = 0; i < take; ++i) {
        auto memEntry = memtableCandidates.back();
        Record r;
        r.key = memEntry.key;
        r.value = memEntry.value;
        r.timestamp = memEntry.timestamp;
        ret.push_back(r);
        memtableCandidates.pop_back();
    }

    end = memtableCandidates.empty();

    return ret;
    
}

void SSTableCursor::prepare_prefix_scan(const std::string& prefix) {
    if(prefix_scan_prepared) return;
    if(range_scan_prepared) reset();


    // Pripremamo kandidate iz memtabele
    memtableCandidates = memt_manager->getAllEntries();
    vector<MemtableEntry> temp;

    for(auto r : memtableCandidates) {
        if(r.key.rfind(prefix, 0) == 0) {
            temp.push_back(move(r));
        }
    }

    memtableCandidates = temp;
    
    std::sort(memtableCandidates.begin(), memtableCandidates.end(),  [](auto& a, auto& b){ return a.key > b.key; }); 

    for(int i = 0; i < sstables.size(); ++i) {
        if(sstables[i]->getSummaryMax() < prefix) continue;

        Record r;
        
        if(sstables[i]->getSummaryMin() > prefix) {
            r = *sst_iterators[i];
            if(r.key.rfind(prefix, 0) != 0) {
                continue; // key ne pocinje sa prefiksom
            }
        }

        bool in_file;
        uint64_t start_offset = sstables[i]->findRecordOffset(prefix, in_file); // Trebalo bi da vraca najblizi desni rekord

        sst_iterators[i].seek(start_offset);
        r = *sst_iterators[i];

        if(r.key.rfind(prefix, 0) != 0) {
                continue;
            }

        candidates.emplace_back(sstables[i], sst_iterators[i], r);
    }

    prefix_scan_prepared = true;
}

void SSTableCursor::prepare_range_scan(const std::string& min_key,const std::string& max_key) {
    if(range_scan_prepared) return;
    if(prefix_scan_prepared) reset();


    // Pripremamo kandidate iz memtabele
    memtableCandidates = memt_manager->getAllEntries();
    vector<MemtableEntry> temp;

    for(auto r : memtableCandidates) {
        if(r.key <= max_key && r.key >= min_key) {
            temp.push_back(move(r));
        }
    }

    memtableCandidates = temp;
    
    std::sort(memtableCandidates.begin(), memtableCandidates.end(),  [](auto& a, auto& b){ return a.key > b.key; }); 


    for(int i = 0; i < sstables.size(); ++i) {
        if(sstables[i]->getSummaryMax() < min_key || sstables[i]->getSummaryMin() > max_key) continue;

        bool in_file;
        uint64_t start_offset = sstables[i]->findRecordOffset(min_key, in_file);

        sst_iterators[i].seek(start_offset);
        Record r = *sst_iterators[i];

        candidates.emplace_back(sstables[i], sst_iterators[i], r);
    }

    range_scan_prepared = true;
}


void SSTableCursor::reset(){
    candidates.clear();
    for(int i = 0; i < sstables.size(); i++) {
        candidates.emplace_back(sstables[i], sst_iterators[i], *(sst_iterators[i]));
    }
    range_scan_prepared = false;
    prefix_scan_prepared = false;
}