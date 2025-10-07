#pragma once
#include "../System/System.h"

class TypesMenu {
public:
    TypesMenu(System* system);

    void showMenu();
    void handleBloomFilter();
    void handleCountMinSketch();
    void handleHyperLogLog();
    void handleSimHash();

	int getIntInput(const std::string& prompt);
	double getDoubleInput(const std::string& prompt);

private:
    System* system;
    TypesManager* typesManager;

    void showBloomFilterMenu();
    void showCountMinSketchMenu();
    void showHyperLogLogMenu();
    void showSimHashMenu();
};