#pragma once

#include "System.h"

class TypesMenu {
public:
    explicit TypesMenu(System* system);

    void showMenu();
    void handleBloomFilter();
    void handleCountMinSketch();
    void handleHyperLogLog();
    void handleSimHash();

private:
    System* system;

    void showBloomFilterMenu();
    void showCountMinSketchMenu();
    void showHyperLogLogMenu();
    void showSimHashMenu();
};