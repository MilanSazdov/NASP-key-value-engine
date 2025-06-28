#pragma once

#include "Compaction.h"

class SizeTieredCompactionStrategy : public CompactionStrategy {
public:
    SizeTieredCompactionStrategy(int min_threshold, int max_threshold);

    std::optional<CompactionPlan> pickCompaction(
        const std::vector<std::vector<SSTableMetadata>>& levels
    ) const override;

private:
    int min_threshold_;
    int max_threshold_;
};