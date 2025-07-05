#include "VarEncoding.h"

template<typename UInt>
std::string varenc::encodeVarint(UInt value) {

    std::string out;
    out.reserve(sizeof(UInt));

    do { // Uzimamo najmanjih 7 bitova, shiftujemo original
        uint8_t byte = value & 0x7F; 
        value >>= 7;
        if (value != 0) 
            byte |= 0x80; // Ako nema vise setujemo flag na kraju
        out.push_back(static_cast<char>(byte));
    } while (value != 0);

    return out;
}

template<typename UInt>
bool varenc::decodeVarint(char byte, UInt& value, size_t& bitOffset) {
    bool more = (byte & 0x80) != 0;
    byte &= 0x7F;
    value |= (UInt(byte) << bitOffset);
    bitOffset += 7;
    return !more;
}