#include "MainApp.h"

/*
    what to configure in config file:

        block_size (block_manager.h)
        cache_capacity (cache.h)
        segment_size (wal.h)
        vrv jos neke gluposti oko sstable, lsm nivoa, velicine memtablea i to (ask andrej and milan)
*/

int main() {
 
    MainApp app;
    app.test_case();

    return 0;
}
