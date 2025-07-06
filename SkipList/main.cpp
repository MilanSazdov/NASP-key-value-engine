#include <iostream>
#include <cassert> // za provere ispravnosti
#include "SkipList.h"

// --- Definisanje ANSI kodova za boje ---
const std::string RESET = "\033[0m";
const std::string BOLD_GREEN = "\033[1;32m";
const std::string BOLD_CYAN = "\033[1;36m";
const std::string YELLOW = "\033[0;33m";
const std::string BLUE = "\033[0;34m";
const std::string MAGENTA = "\033[0;35m";

// pomocna funkcija za ispisivanje trenutnog stanja skip liste
void printSkipListState(const SkipList& list) {
    std::cout << BLUE << "--- Trenutno stanje Skip Liste (Size: " << list.Size() << ") ---" << RESET << std::endl;
    auto all_entries = list.getAllEntries(); // Koristimo novu, efikasnu metodu
    if (all_entries.empty()) {
        std::cout << "  Lista je prazna." << std::endl;
    }
    for (const auto& entry : all_entries) {
        std::cout << "  Key: " << entry.key
            << ", Value: '" << entry.value << "'"
            << ", Tombstone: " << (entry.tombstone ? "true" : "false")
            << ", Timestamp: " << entry.timestamp << std::endl;
    }
    std::cout << BLUE << "------------------------------------------\n" << RESET << std::endl;
}

int main() {

    // 1. Inicijalizacija SkipList instance
    std::cout << YELLOW << "Kreiranje SkipList instance..." << RESET << std::endl;
    SkipList sl(16, 0.5);
    printSkipListState(sl);
    assert(sl.Size() == 0);

    // 2. Testiranje inserta
    std::cout << BOLD_GREEN << "===== TEST 1: Osnovno Umetanje (insert) =====\n" << RESET;
    sl.insert("banana", "zuta", false, 100);
    sl.insert("jabuka", "crvena", false, 101);
    sl.insert("narandza", "narandzasta", false, 102);
    sl.insert("grozdje", "zeleno", false, 103);
    printSkipListState(sl);
    assert(sl.Size() == 4);

    // 3. Testiranje get
    std::cout << BOLD_GREEN << "===== TEST 2: Pretraga (get) =====\n" << RESET;
    std::cout << YELLOW << "Trazim kljuc 'jabuka'..." << RESET << std::endl;
    auto value = sl.get("jabuka");
    assert(value.has_value() && value.value() == "crvena");
    std::cout << "Pronadjena vrednost: " << value.value() << std::endl;

    std::cout << YELLOW << "\nTrazim kljuc 'kruska' (ne postoji)..." << RESET << std::endl;
    auto non_existent_value = sl.get("kruska");
    assert(!non_existent_value.has_value());
    std::cout << "Vrednost nije pronadjena, sto je ocekivano.\n" << std::endl;

    // 4. Testiranje azuriranja postojeceg ključa
    std::cout << BOLD_GREEN << "===== TEST 3: Azuriranje Vrednosti =====\n" << RESET;
    std::cout << YELLOW << "Azuriram 'jabuka' na 'zelena'..." << RESET << std::endl;
    sl.insert("jabuka", "zelena", false, 105);
    printSkipListState(sl);
    value = sl.get("jabuka");
    assert(sl.Size() == 4); // Velicina se ne menja
    assert(value.has_value() && value.value() == "zelena");
    std::cout << "Nova vrednost za 'jabuka' je: " << value.value() << std::endl;

    // 5. Testiranje logickog brisanja (Tombstone)
    std::cout << BOLD_GREEN << "\n===== TEST 4: Logicko Brisanje (Tombstone) =====\n" << RESET;
    std::cout << YELLOW << "Logicki brisem 'banana'..." << RESET << std::endl;
    sl.insert("banana", "", true, 110); // Postavljamo tombstone
    printSkipListState(sl);

    auto deleted_value = sl.get("banana");
    std::cout << YELLOW << "Pokusaj dobavljanja obrisanog kljuca 'banana'..." << RESET << std::endl;
    assert(!deleted_value.has_value());
    std::cout << "Kljuc nije dobavljen, sto je ocekivano." << std::endl;

    auto banana_node = sl.getNode("banana");
    assert(banana_node != nullptr && banana_node->tombstone == true);
    std::cout << "Provera cvora 'banana' potvrdjuje da je tombstone postavljen na true." << std::endl;
    assert(sl.Size() == 4); // Velicina se i dalje ne menja!

    // 6. Testiranje fizickog brisanja (remove)
    std::cout << BOLD_GREEN << "\n===== TEST 5: Fizicko Brisanje (remove) =====\n" << RESET;
    std::cout << YELLOW << "Fizicki brisem 'narandza'..." << RESET << std::endl;
    sl.remove("narandza");
    printSkipListState(sl);
    assert(sl.Size() == 3); // Velicina se sada smanjila
    assert(sl.getNode("narandza") == nullptr);
    std::cout << "Cvor 'narandza' je fizicki uklonjen." << std::endl;

    std::cout << YELLOW << "\nPokusaj brisanja nepostojeceg kljuca 'kruska'..." << RESET << std::endl;
    sl.remove("kruska");
    assert(sl.Size() == 3); // Velicina ostaje ista
    std::cout << "Velicina liste nepromenjena, sto je ocekivano." << std::endl;

    std::cout << MAGENTA << "\nSvi testovi su uspesno prosli!" << RESET << std::endl;

    return 0;
}
