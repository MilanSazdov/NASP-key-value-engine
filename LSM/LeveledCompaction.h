#pragma once

#include "Compaction.h"

class LeveledCompactionStrategy : public CompactionStrategy {
public:
    LeveledCompactionStrategy(int l0_compaction_trigger, int max_levels, uint64_t target_file_size_base, int level_size_multiplier);

    std::optional<CompactionPlan> pickCompaction(
        const std::vector<std::vector<SSTableMetadata>>& levels
    ) const override;

private:
    uint64_t calculate_level_target_size(int level, uint64_t total_db_size) const;
    std::vector<SSTableMetadata> find_overlapping_files(const SSTableMetadata& sst, const std::vector<SSTableMetadata>& level_files) const;
    std::vector<SSTableMetadata> find_overlapping_files_for_range(const std::string& min_key, const std::string& max_key, const std::vector<SSTableMetadata>& level_files) const;

    int l0_compaction_trigger_;
    int max_levels_;
    uint64_t target_file_size_base_;
    int level_size_multiplier_;
};

