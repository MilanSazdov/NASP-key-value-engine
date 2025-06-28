#include "LeveledCompaction.h"
#include <numeric>
#include <algorithm>

LeveledCompactionStrategy::LeveledCompactionStrategy(int l0_compaction_trigger, int max_levels, uint64_t target_file_size_base, int level_size_multiplier)
    : l0_compaction_trigger_(l0_compaction_trigger),
    max_levels_(max_levels),
    target_file_size_base_(target_file_size_base),
    level_size_multiplier_(level_size_multiplier) {
}

// dinamicki izracunava ciljanu velicinu nivoa na osnovu ukupne velicine baze
uint64_t LeveledCompactionStrategy::calculate_level_target_size(int level, uint64_t total_db_size) const {
    if (level == 0) return 0; // L0 nema ciljanu velicinu

    uint64_t size = total_db_size;
    for (int i = level; i < max_levels_ - 1; ++i) {
        size /= level_size_multiplier_;
    }
    return std::max(size, target_file_size_base_);
}

// pronalazi fajlove u nivou koji se preklapaju sa opsegom datog SSTable-a
std::vector<SSTableMetadata> LeveledCompactionStrategy::find_overlapping_files(const SSTableMetadata& sst, const std::vector<SSTableMetadata>& level_files) const {
    return find_overlapping_files_for_range(sst.min_key, sst.max_key, level_files);
}

// generalizovana funkcija za pronalazenje preklapanja sa opsegom.
std::vector<SSTableMetadata> LeveledCompactionStrategy::find_overlapping_files_for_range(
    const std::string& min_key, const std::string& max_key, const std::vector<SSTableMetadata>& level_files
) const {
    std::vector<SSTableMetadata> overlapping;
    for (const auto& file : level_files) {
        // provera da li se opsezi [min_key, max_key] i [file.min_key, file.max_key] preklapaju
        if (!(max_key < file.min_key || min_key > file.max_key)) {
            overlapping.push_back(file);
        }
    }
    return overlapping;
}

std::optional<CompactionPlan> LeveledCompactionStrategy::pickCompaction(
    const std::vector<std::vector<SSTableMetadata>>& levels
) const {
    // --- 1. Provera za L0 -> L1 kompakciju ---
    // levels[0] je vektor koji sadrzi sve fajlove na Nivou 0.
    if (!levels.empty() && levels[0].size() >= l0_compaction_trigger_) {
        CompactionPlan plan;
        plan.inputs_level_1 = levels[0]; // ulaz su SVI fajlovi sa nivoa 0.
        plan.output_level = 1;

        // pronadji sve fajlove u L1 koji se preklapaju sa opsegom BILO KOG fajla iz L0.
        if (levels.size() > 1 && !levels[0].empty()) {

            // ===== ISPRAVKA 1: Pronalazenje ukupnog opsega za L0 =====
            // inicijalizuj min/max sa vrednostima iz prvog fajla.
            std::string min_key_l0 = levels[0][0].min_key;
            std::string max_key_l0 = levels[0][0].max_key;

            // iteriraj kroz ostatak fajlova u L0 da pronadjes pravi minimum i maksimum.
            for (size_t i = 1; i < levels[0].size(); ++i) {
                if (levels[0][i].min_key < min_key_l0) {
                    min_key_l0 = levels[0][i].min_key;
                }
                if (levels[0][i].max_key > max_key_l0) {
                    max_key_l0 = levels[0][i].max_key;
                }
            }
            // sada kada imamo pravi opseg za ceo L0, trazimo preklapanje u L1 (levels[1]).
            plan.inputs_level_2 = find_overlapping_files_for_range(min_key_l0, max_key_l0, levels[1]);
        }
        return plan;
    }

    // --- 2. Provera za Li -> L(i+1) kompakcije (za i > 0) ---
    uint64_t total_db_size = 0;
    for (const auto& level : levels) {
        for (const auto& sst : level) {
            total_db_size += sst.file_size;
        }
    }

    for (int i = 1; i < max_levels_ - 1; ++i) {
        if (levels.size() <= i || levels[i].empty()) continue;

        uint64_t current_level_size = 0;
        for (const auto& sst : levels[i]) {
            current_level_size += sst.file_size;
        }

        if (current_level_size > calculate_level_target_size(i, total_db_size)) {
            CompactionPlan plan;

            // ===== ISPRAVKA 2: Odaberi SAMO JEDAN fajl iz ovog nivoa =====
            // najjednostavnija strategija je uzeti prvi, ali u realnosti bi se koristio
            // "round-robin" pokazivac da se osigura da se svi fajlovi vremenom kompaktuju
            const SSTableMetadata& file_to_compact = levels[i][0];
            plan.inputs_level_1.push_back(file_to_compact);
            plan.output_level = i + 1;

            // pronadji preklapajuce fajlove u sledecem nivou (levels[i + 1])
            if (levels.size() > i + 1) {
                plan.inputs_level_2 = find_overlapping_files(file_to_compact, levels[i + 1]);
            }
            return plan;
        }
    }

    return std::nullopt; // Nema posla
}