#include "SizeTieredCompaction.h"
#include <map>
#include <algorithm>

SizeTieredCompactionStrategy::SizeTieredCompactionStrategy(int min_threshold, int max_threshold)
    : min_threshold_(min_threshold), max_threshold_(max_threshold) {
}

// pomocna funkcija za grupisanje SSTable-ova u buckete (tiers) po velicini.
std::map<uint64_t, std::vector<SSTableMetadata>> group_by_size(const std::vector<SSTableMetadata>& sstables) {
    std::map<uint64_t, std::vector<SSTableMetadata>> tiers;
    if (sstables.empty()) return tiers;

    // jednostavna logika: grupisi po redu velicine (npr. 1MB, 2MB, 4MB, 8MB...)
    for (const auto& sst : sstables) {
        uint64_t tier_size = 1;
        // pronadji najblizi stepen dvojke koji je veci ili jednak velicini fajla
        while (tier_size < sst.file_size && tier_size < (1ULL << 62)) {
            tier_size *= 2;
        }
        tiers[tier_size].push_back(sst);
    }
    return tiers;
}

std::optional<CompactionPlan> SizeTieredCompactionStrategy::pickCompaction(
    const std::vector<std::vector<SSTableMetadata>>& levels
) const {
    // U STCS, svi nivoi se tretiraju kao jedan veliki bazen fajlova
    std::vector<SSTableMetadata> all_sstables;
    for (const auto& level : levels) {
        all_sstables.insert(all_sstables.end(), level.begin(), level.end());
    }

    auto tiers = group_by_size(all_sstables);

    // pronadji najbolji tier za kompakciju (onaj sa najvise fajlova koji prelazi prag)
    std::optional<std::vector<SSTableMetadata>> best_tier_to_compact;
    size_t max_files_in_tier = 0;

    for (const auto& pair : tiers) {
        if (pair.second.size() >= min_threshold_ && pair.second.size() > max_files_in_tier) {
            max_files_in_tier = pair.second.size();
            best_tier_to_compact = pair.second;
        }
    }

    if (best_tier_to_compact) {
        CompactionPlan plan;
        plan.inputs_level_1 = *best_tier_to_compact;
        // u STCS, izlaz ide na isti "logicki" nivo, ali ce se kasnije regrupisati po velicini
        // za jednostavnost, mozemo reci da izlaz ide na nivo 0, odakle ce se ponovo klasifikovati
        plan.output_level = 0;
        return plan;
    }

    return std::nullopt;
}