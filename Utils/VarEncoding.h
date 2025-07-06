#pragma once
#include <string>
#include <cstdint>

namespace varenc {
    template<typename UInt>
    std::string encodeVarint(UInt input);

    template<typename UInt>
    bool decodeVarint(char input, UInt& value, size_t& offset);

    template<typename UInt>
    std::string varenc::encodeVarint(UInt value) {

        std::string out;
        out.reserve(sizeof(UInt));

        do {
            uint8_t chunk = value & 0x7F; 
            value >>= 7;
            if (value != 0) 
                chunk |= 0x80; // Ako ima jos stavljamo zastavicu
            out.push_back(static_cast<char>(chunk));
        } while (value != 0);

        return out;
    }

    template<typename UInt>
    bool varenc::decodeVarint(char chunk, UInt& value, size_t& bitOffset) {
        bool more = (chunk & 0x80) != 0;
        chunk &= 0x7F;
        value |= (UInt(chunk) << bitOffset);
        bitOffset += 7;
        return !more;
    }

}