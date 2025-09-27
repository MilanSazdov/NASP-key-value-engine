#pragma once

//#include "TypesManager.h"
#include "../System/System.h"

class TypesMenu {
public:
    TypesMenu(System* system);

    void showMenu();
    void handleBloomFilter();
    void handleCountMinSketch();
    void handleHyperLogLog();
    void handleSimHash();

private:
    System* system;
    TypesManager* typesManager;

    void showBloomFilterMenu();
    void showCountMinSketchMenu();
    void showHyperLogLogMenu();
    void showSimHashMenu();
};