#pragma once
#include <string>
#include <cstdint> // For fixed-width integers

typedef long long ll;
typedef unsigned long long ull;
typedef unsigned int uint;

const string LOG_DIRECTORY = "../data/wal_logs/";

// This is used for fragmentation
enum class Wal_record_type : uint8_t {
	FULL = (byte)65,
	FIRST = (byte)66,
	MIDDLE = (byte)67,
	LAST = (byte)68
};

inline string record_type_to_string(Wal_record_type type) {
	switch (type) {
		case Wal_record_type::FULL:   return "FULL";
		case Wal_record_type::FIRST:  return "FIRST";
		case Wal_record_type::MIDDLE: return "MIDDLE";
		case Wal_record_type::LAST:   return "LAST";
		default:                      return "UNKNOWN";
	}
}

// This struct represents a record AFTER it has been read from the log
// and deserialized into memory. It is NOT the on-disk format.
struct Record {
	uint crc;
	ull timestamp;
	std::byte tombstone;

	ull key_size;
	ull value_size;

	// Wal_record_type flag;

	std::string key;
	std::string value;
};