#pragma once

#include "Compaction.h"

class SizeTieredCompactionStrategy : public CompactionStrategy {
public:
    SizeTieredCompactionStrategy(int threshold);

    std::optional<CompactionPlan> pickCompaction(
        const std::vector<std::vector<SSTableMetadata>>& levels
    ) const override;

private:
    int threshold_;
};