#include "SSTableSingleFile.h"
#include "Config.h"
#include <stdexcept>
#include <iostream>
#include <sstream>
#include <algorithm>

SSTableSingleFile::SSTableSingleFile(const std::string& single_file_path, Block_manager* bmp, bool is_compressed)
// Prosledjujemo istu putanju za sve fajlove, jer je sve u jednom
    : SSTable(single_file_path, single_file_path, single_file_path, single_file_path, single_file_path, bmp)
{
    // Postavljamo fleg za kompresiju u TOC
    if (is_compressed) {
        toc_.flags |= 1;
    }
}

void SSTableSingleFile::readTOC() {
    if (toc_loaded_) return;

    std::vector<byte> toc_buffer(sizeof(TOC));
    bool error = false;

    // citamo prvi blok gde se TOC nalazi
    auto block = bmp->read_block({ 0, dataFile_ }, error);
    if (error || block.size() < sizeof(TOC)) {
        throw std::runtime_error("Failed to read TOC from SSTable single file.");
    }

    memcpy(&toc_, block.data(), sizeof(TOC));

    // Provera magicnog broja
    if (toc_.magic_number != 0xABCDDCBA12344321) {
        throw std::runtime_error("Invalid SSTable single file format (magic number mismatch).");
    }

    // Postavljamo interne promenljive na osnovu procitanog TOC-a
    this->block_size = toc_.saved_block_size;
    this->SPARSITY = toc_.saved_sparsity;

    toc_loaded_ = true;
}


// cita odredjenu komponentu iz velikog fajla
std::vector<byte> SSTableSingleFile::readComponent(uint64_t component_offset, uint64_t component_len) {
    if (!toc_loaded_) readTOC(); // Osiguraj da je TOC ucitan

    std::vector<byte> buffer(component_len);
    uint64_t bytes_read = 0;
    bool error = false;

    while (bytes_read < component_len) {
        // Racunamo u kom se bloku nalaze podaci koji nam trebaju
        int block_id = (component_offset + bytes_read) / this->block_size;
        size_t offset_in_block = (component_offset + bytes_read) % this->block_size;

        auto block = bmp->read_block({ block_id, dataFile_ }, error);
        if (error) throw std::runtime_error("Greska pri citanju bloka tokom citanja komponente.");

        // Koliko bajtova mozemo da prekopiramo iz ovog bloka?
        size_t bytes_to_copy = std::min((size_t)(component_len - bytes_read), (size_t)this->block_size - offset_in_block);

        // Kopiramo deo bloka u nas izlazni bafer
        memcpy(buffer.data() + bytes_read, block.data() + offset_in_block, bytes_to_copy);
        bytes_read += bytes_to_copy;
    }
    return buffer;
}

void SSTableSingleFile::build(std::vector<Record>& records) {
    std::cout << "[SSTableSingleFile] Building single-file SSTable..." << std::endl;

    // KORAK 1: Gradnja komponenti u memorijskim baferima
    std::stringstream data_stream, index_stream, summary_stream, filter_stream, meta_stream;

    // Potrebno je privremeno modifikovati vaše write* metode da umesto u Block Manager
    // pišu u ove stringstream objekte. Ovo je najveći deo posla.
    // Primer kako bi to izgledalo (pseudo-kod):
    // writeDataToBuffer(records, data_stream);
    // writeIndexToBuffer(index_entries, index_stream);
    // ... itd.

    // Za sada, pretpostavimo da su baferi popunjeni (OVO JE VAŠ GLAVNI ZADATAK ZA IMPLEMENTACIJU)
    // std::string data_content = ...;
    // std::string index_content = ...;

    // KORAK 2: Izračunaj veličine i ofsete
    toc_.block_size = Config::block_size;
    toc_.sparsity = Config::index_sparsity;

    // toc_.data_len = data_content.length();
    // toc_.index_len = index_content.length();
    // ...

    toc_.data_offset = sizeof(TOC);
    // toc_.index_offset = toc_.data_offset + toc_.data_len;
    // ...

    // KORAK 3: Upiši sve na disk
    std::vector<byte> toc_buffer(sizeof(TOC));
    memcpy(toc_buffer.data(), &toc_, sizeof(TOC));

    // Pišemo TOC i sve komponente redom
    bmp->write_block({ 0, dataFile_ }, toc_buffer); // Uvek pocinje od bloka 0
    // bmp->write_string_in_blocks(dataFile_, data_content, 1); // Pomoćna funkcija koja upisuje string preko više blokova
    // bmp->write_string_in_blocks(dataFile_, index_content, ...);
}
