#include "SizeTieredCompaction.h"
#include <map>
#include <algorithm>

SizeTieredCompactionStrategy::SizeTieredCompactionStrategy(int threshold)
    : threshold_(threshold) {
}

std::optional<CompactionPlan> SizeTieredCompactionStrategy::pickCompaction(
    const std::vector<std::vector<SSTableMetadata>>& levels
) const {

    for (size_t i = 0; i < levels.size() - 1; ++i) {
        if (levels[i].size() >= threshold_) {

            std::cout << "[SizeTieredStrategy] Nivo " << i << " ima " << levels[i].size()
                << " fajlova (prag je " << threshold_ << "). Pokrecem kompakciju." << std::endl;

            CompactionPlan plan;

            plan.inputs_level_1 = levels[i];
            plan.output_level = i + 1;

            return plan;
        }
    }

    return std::nullopt;
}