#pragma once
#include <string>

namespace varenc {
    template<typename UInt>
    std::string encodeVarint(UInt input);

    template<typename UInt>
    bool decodeVarint(char input, UInt& value, size_t& offset);
}