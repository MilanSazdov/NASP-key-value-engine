#include "wal.h"
Wal::Wal() {
	segment_size = 4096;
}
Wal::Wal(int segment_size) {
	this->segment_size = segment_size;
}