#include "TypesMenu.h"
#include <iostream>

TypesMenu::TypesMenu(System* system) : system(system) {}

void TypesMenu::showMenu() {
    int choice;
    do {
        std::cout << "\n========== Special Data Types ==========\n";
        std::cout << "1. Bloom Filter Operations\n";
        std::cout << "2. Count-Min Sketch Operations\n";
        std::cout << "3. HyperLogLog Operations\n";
        std::cout << "4. SimHash Operations\n";
        std::cout << "5. Back to Main Menu\n";
        std::cout << "======================================\n";
        std::cout << "Enter choice: ";
        std::cin >> choice;
        std::cin.ignore();

        switch (choice) {
        case 1: handleBloomFilter(); break;
        case 2: handleCountMinSketch(); break;
        case 3: handleHyperLogLog(); break;
        case 4: handleSimHash(); break;
        case 5: return;
        default: std::cout << "Invalid choice.\n";
        }
    } while (choice != 5);
}

void TypesMenu::showBloomFilterMenu() {
    std::cout << "\n========== Bloom Filter ==========\n";
    std::cout << "1. New BF \n";
    std::cout << "2. Delete BF \n";
    std::cout << "3. BF Add \n";
    std::cout << "4. BF HasKey \n";
    std::cout << "5. Back\n";
    std::cout << "================================\n";
}

void TypesMenu::handleBloomFilter() {
    int choice;
    do {
        showBloomFilterMenu();
        std::cout << "Enter choice: ";
        std::cin >> choice;
        std::cin.ignore();

        switch (choice) {
        case 1: {
            std::string key;
            int n;
            double p;

            std::cout << "Enter key: ";
            std::getline(std::cin, key);

            // TODO: Check for int values
            std::cout << "Enter number of elements (n): ";
            std::cin >> n;

            std::cout << "Enter false positivity probability (p): ";
            std::cin >> p;
            std::cin.ignore(); // To consume the newline

            // TODO: Create Bloom Filter with these parameters, check if it already exist
            std::cout << "Bloom Filter created with key: " << key
                << ", n: " << n << ", p: " << p << std::endl;
            break;
        }
        case 2: {
            std::string key;
            std::cout << "Enter key to delete: ";
            std::getline(std::cin, key);
            // TODO: Delete Bloom Filter
            std::cout << "Bloom Filter with key: " << key << " deleted" << std::endl;
            break;
        }
        case 3: {
            std::string key, value;
            std::cout << "Enter key: ";
            std::getline(std::cin, key);
            std::cout << "Enter value to add: ";
            std::getline(std::cin, value);
            // TODO: Add value to Bloom Filter with the given key
            break;
        }
        case 4: {
            std::string key, value;
            std::cout << "Enter key: ";
            std::getline(std::cin, key);
            std::cout << "Enter value to check: ";
            std::getline(std::cin, value);
            // TODO: Check if value exists in Bloom Filter with the given key
            break;
        }
        case 5: return;
        default: std::cout << "Invalid choice.\n";
        }
    } while (choice != 5);
}

void TypesMenu::showCountMinSketchMenu() {
    std::cout << "\n========== Count-Min Sketch ==========\n";
    std::cout << "1. New CMS \n";
    std::cout << "2. Delete CMS\n";
    std::cout << "3. CMS Add \n";
    std::cout << "4. CMS Get \n";
    std::cout << "5. Back\n";
    std::cout << "======================================\n";
}

void TypesMenu::handleCountMinSketch() {
    int choice;
    do {
        showCountMinSketchMenu();
        std::cout << "Enter choice: ";
        std::cin >> choice;
        std::cin.ignore();
        switch (choice) {
        case 1:
        {
            std::string key;
            double epsilon, delta;
            std::cout << "Enter key: ";
            std::getline(std::cin, key);
            std::cout << "Enter epsilon: ";
            std::cin >> epsilon;
            std::cout << "Enter delta: ";
            std::cin >> delta;
            std::cin.ignore(); // To consume the newline

            std::cout << "Count-Min Sketch created with key: " << key
                << ", epsilon: " << epsilon << ", delta: " << delta << std::endl;

            // TODO: Create Count-Min Sketch with these parameters, check if it already exists
            break;
        }
        case 2: {
            std::string key;
            std::cout << "Enter key to delete: ";
            std::getline(std::cin, key);

            std::cout << "Count-Min Sketch with key: " << key << " deleted" << std::endl;

            // TODO: Delete Count-Min Sketch    
            break;
        }
        case 3: {
            std::string key, value;
            std::cout << "Enter key: ";
            std::getline(std::cin, key);
            std::cout << "Enter value to add: ";
            std::getline(std::cin, value);

            // TODO: Add value to Count-Min Sketch with the given key
            break;
        }
        case 4: {
            std::string key, value;
            std::cout << "Enter key: ";
            std::getline(std::cin, key);
            std::cout << "Enter value to get: ";
            std::getline(std::cin, value);

            // TODO: Get value from Count-Min Sketch with the given key
            break;
        }
        case 5: return;
        default: std::cout << "Invalid choice.\n";
        }
    } while (choice != 5);
}

void TypesMenu::showHyperLogLogMenu() {
    std::cout << "\n========== HyperLogLog ==========\n";
    std::cout << "1. New HLL\n";
    std::cout << "2. Delete HLL\n";
    std::cout << "3. HLL Add\n";
    std::cout << "4. HLL Estimate\n";
    std::cout << "5. Back\n";
    std::cout << "==================================\n";
}

void TypesMenu::handleHyperLogLog() {
    int choice;
    do {
        showHyperLogLogMenu();
        std::cout << "Enter choice: ";
        std::cin >> choice;
        std::cin.ignore();
        switch (choice) {
        case 1: {
            std::string key;
            int p;
            std::cout << "Enter key: ";
            std::getline(std::cin, key);
            std::cout << "Enter precision (p): ";
            std::cin >> p;
            std::cin.ignore(); // To consume the newline

            std::cout << "HyperLogLog created with key: " << key << ", p: " << p << std::endl;
            // TODO: Create HyperLogLog with these parameters, check if it already exists
            break;
        }
        case 2: {
            std::string key;
            std::cout << "Enter key to delete: ";
            std::getline(std::cin, key);

            std::cout << "HyperLogLog with key: " << key << " deleted" << std::endl;
            // TODO: Delete HyperLogLog

            break;
        }
        case 3: {
            std::string key, value;
            std::cout << "Enter key: ";
            std::getline(std::cin, key);
            std::cout << "Enter value to add: ";
            std::getline(std::cin, value);

            // TODO: Add value to HyperLogLog with the given key
            break;
        }
        case 4: {
            std::string key;
            std::cout << "Enter key to estimate: ";
            std::getline(std::cin, key);

            // TODO: Estimate cardinality of HyperLogLog with the given key
            break;
        }
        case 5: return;
        default: std::cout << "Invalid choice.\n";
        }
    } while (choice != 5);
}

void TypesMenu::showSimHashMenu() {
    std::cout << "\n========== SimHash ==========\n";
    std::cout << "1. SH Add Fingerprint\n";
    std::cout << "2. SH Delete Fingerprint\n";
    std::cout << "3. SH Get Hamming Distance\n";
    std::cout << "4. Back\n";
    std::cout << "=============================\n";
}

void TypesMenu::handleSimHash() {
    int choice;
    do {
        showSimHashMenu();
        std::cout << "Enter choice: ";
        std::cin >> choice;
        std::cin.ignore();
        switch (choice) {
        case 1: {
            std::string key, text;
            std::cout << "Enter key: ";
            std::getline(std::cin, key);
            std::cout << "Enter text to add: ";
            std::getline(std::cin, text);

            // TODO: Add fingerprint to SimHash with the given key
            break;
        }
        case 2: {
            std::string key;
            std::cout << "Enter key to delete: ";
            std::getline(std::cin, key);

            // TODO: Delete fingerprint from SimHash with the given key
            break;
        }
        case 3: {
            std::string key1, key2;
            std::cout << "Enter first key: ";
            std::getline(std::cin, key1);
            std::cout << "Enter second key: ";
            std::getline(std::cin, key2);

            // TODO: Get Hamming distance between fingerprints of the two keys
            break;
        }
        case 4: return;
        default: std::cout << "Invalid choice.\n";
        }
    } while (choice != 4);
}