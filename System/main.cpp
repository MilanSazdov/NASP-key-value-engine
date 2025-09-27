#include "System.h"
#include <iostream>

void input_test_data(Wal*& w1) {
    w1->put("apple", "fruit");
    w1->put("dog", "animal");
    w1->put("rose", "flower");
    w1->put("carrot", "vegetable");
    w1->put("oak", "tree");
    w1->put("sparrow", "bird");
    w1->put("shark", "fish");
    w1->put("iron", "metal");
    w1->put("mercury", "planet");
    w1->put("violin", "instrument");
    w1->put("blue", "color");
    w1->put("python", "language");
    w1->put("java", "coffee");
    w1->put("winter", "season");
    w1->put("happiness", "emotion");
    w1->put("oxygen", "element");
    w1->put("computer", "machine");
    w1->put("earth", "planet");
    w1->put("gold", "metal");
    w1->put("diamond", "gem");
    w1->put("river", "water");
    w1->put("mountain", "landform");
    w1->put("sun", "star");
    w1->put("moon", "satellite");
    w1->put("paper", "material");
    w1->put("pen", "tool");
    w1->put("book", "object");
    w1->put("courage", "virtue");
    w1->put("freedom", "concept");
    w1->put("justice", "ideal");
    w1->put("laptop", "device");
    w1->put("rain", "weather");
    w1->put("cloud", "sky");
    w1->put("time", "dimension");
    w1->put("speed", "quantity");
    w1->put("energy", "power");
    w1->put("peace", "state");
    w1->put("love", "feeling");
    w1->put("music", "art");
    w1->put("dream", "thought");
}

int main() {

    std::cout << "[Main] Launching system...\n";
    System sys;
    


    std::cout << "[Main] System launched successfully.\n";


    // Dodavanje novih ključeva i vrednosti
    /*
    std::cout << "[Main] Dodavanje kljuceva u Memtable...\n";
    system.put("key1", "value1", false);
    system.put("key2", "value2", false);
    system.put("key3", "value3", false);
    system.put("key3", "value3", true);
    */
    /*
    // Ažuriranje postojeće vrednosti
    std::cout << "[Main] Ažuriranje vrednosti za ključ 'key2'...\n";
    system.put("key2", "new_value2", false);

    // Dodavanje više ključeva da popunimo Memtable
    std::cout << "[Main] Dodavanje dodatnih ključeva...\n";
    system.put("key4", "value4", false);
    system.put("key5", "value5", false);
    system.put("key6", "value6", false);

    // Testiranje tombstone
    std::cout << "[Main] Brisanje ključa 'key3' sa tombstone...\n";
    system.put("key3", "", true); // Obeležavanje ključa 'key3' kao obrisanog

    // Dodavanje novih ključeva posle brisanja
    system.put("key7", "value7", false);
    system.put("key8", "value8", false);

    // Ažuriranje ključa koji je bio obrisan
    std::cout << "[Main] Ponovno dodavanje ključa 'key3'...\n";
    system.put("key3", "restored_value3", true);

    // Ispis svih podataka u Memtable
    std::cout << "\n[Main] Svi podaci u Memtable nakon testiranja:\n";
    */
    // system.memtable->printAllData();

    return 0;
}