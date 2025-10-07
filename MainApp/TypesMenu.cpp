#include "TypesMenu.h"
#include <iostream>
#include <limits>

using namespace std;

TypesMenu::TypesMenu(System* sys) {
	system = sys;
    if (!system) {
        throw invalid_argument("System pointer cannot be null");
	} else{
        typesManager = system->getTypesManager();
    }
}

int TypesMenu::getIntInput(const string& prompt) {
    int value;
    while (true) {
        cout << prompt;
        if (cin >> value) {
            cin.ignore(numeric_limits<streamsize>::max(), '\n');
            return value;
        }
        else {
            cout << "Invalid input. Please enter a valid integer.\n";
            cin.clear();
            cin.ignore(numeric_limits<streamsize>::max(), '\n');
        }
    }
}

double TypesMenu::getDoubleInput(const string& prompt) {
    double value;
    while (true) {
        cout << prompt;
        if (cin >> value) {
            cin.ignore(numeric_limits<streamsize>::max(), '\n');
            return value;
        }
        else {
            cout << "Invalid input. Please enter a valid number.\n";
            cin.clear();
            cin.ignore(numeric_limits<streamsize>::max(), '\n');
        }
    }
}


void TypesMenu::showMenu() {
    int choice;
    do {
        cout << "\n========== Special Data Types ==========\n";
        cout << "1. Bloom Filter Operations\n";
        cout << "2. Count-Min Sketch Operations\n";
        cout << "3. HyperLogLog Operations\n";
        cout << "4. SimHash Operations\n";
        cout << "5. Back to Main Menu\n";
        cout << "======================================\n";
        
		choice = getIntInput("Enter choice: ");

        switch (choice) {
        case 1: handleBloomFilter(); break;
        case 2: handleCountMinSketch(); break;
        case 3: handleHyperLogLog(); break;
        case 4: handleSimHash(); break;
        case 5: return;
        default: cout << "Invalid choice.\n";
        }
    } while (choice != 5);
}

void TypesMenu::showBloomFilterMenu() {
    cout << "\n========== Bloom Filter ==========\n";
    cout << "1. New BF \n";
    cout << "2. Delete BF \n";
    cout << "3. BF Add \n";
    cout << "4. BF HasKey \n";
    cout << "5. Back\n";
    cout << "================================\n";
}

void TypesMenu::handleBloomFilter() {
    int choice;
    do {
        showBloomFilterMenu();
        choice = getIntInput("Enter choice: ");

        switch (choice) {
        case 1: {
            string key;
            cout << "Enter key: ";
            getline(cin, key);

            int n = getIntInput("Enter number of elements (n): ");
            double p = getDoubleInput("Enter false positivity probability (p): ");

            typesManager->createBloomFilter(key, n, p);
            break;
        }
        case 2: {
            string key;
            cout << "Enter key to delete: ";
            getline(cin, key);

            typesManager->deleteBloomFilter(key);
            break;
        }
        case 3: {
            string key, value;
            cout << "Enter key: ";
            getline(cin, key);
            cout << "Enter value to add: ";
            getline(cin, value);

            typesManager->addToBloomFilter(key, value);
            break;
        }
        case 4: {
            string key, value;
            cout << "Enter key: ";
            getline(cin, key);
            cout << "Enter value to check: ";
            getline(cin, value);

            typesManager->hasKeyInBloomFilter(key, value);
            break;
        }
        case 5:
            return;
        default:
            cout << "Invalid choice.\n";
        }
    } while (choice != 5);
}


void TypesMenu::showCountMinSketchMenu() {
    cout << "\n========== Count-Min Sketch ==========\n";
    cout << "1. New CMS \n";
    cout << "2. Delete CMS\n";
    cout << "3. CMS Add \n";
    cout << "4. CMS Get \n";
    cout << "5. Back\n";
    cout << "======================================\n";
}

void TypesMenu::handleCountMinSketch() {
    int choice;
    do {
        showCountMinSketchMenu();
		choice = getIntInput("Enter choice: ");

        switch (choice) {
        case 1:
        {
            string key;
            double epsilon, delta;
            cout << "Enter key: ";
            getline(cin, key);
            epsilon = getDoubleInput("Enter epsilon: ");
			delta = getDoubleInput("Enter delta: ");

            typesManager->createCountMinSketch(key, epsilon, delta);
            break;
        }
        case 2: {
            string key;
            cout << "Enter key to delete: ";
            getline(cin, key);

			typesManager->deleteCountMinSketch(key);
            break;
        }
        case 3: {
            string key, value;
            cout << "Enter key: ";
            getline(cin, key);
            cout << "Enter value to add: ";
            getline(cin, value);

			typesManager->addToCountMinSketch(key, value);
            break;
        }
        case 4: {
            string key, value;
            cout << "Enter key: ";
            getline(cin, key);
            cout << "Enter value to get: ";
            getline(cin, value);

			typesManager->getFromCountMinSketch(key, value);
            break;
        }
        case 5: return;
        default: cout << "Invalid choice.\n";
        }
    } while (choice != 5);
}

void TypesMenu::showHyperLogLogMenu() {
    cout << "\n========== HyperLogLog ==========\n";
    cout << "1. New HLL\n";
    cout << "2. Delete HLL\n";
    cout << "3. HLL Add\n";
    cout << "4. HLL Estimate\n";
    cout << "5. Back\n";
    cout << "==================================\n";
}

void TypesMenu::handleHyperLogLog() {
    int choice;
    do {
        showHyperLogLogMenu();
		choice = getIntInput("Enter choice: ");

        switch (choice) {
        case 1: {
            string key;
            int p;
            cout << "Enter key: ";
            getline(cin, key);
			p = getIntInput("Enter precision (p) [4-16]: ");

			typesManager->createHyperLogLog(key, p);
            break;
        }
        case 2: {
            string key;
            cout << "Enter key to delete: ";
            getline(cin, key);

			typesManager->deleteHyperLogLog(key);
            break;
        }
        case 3: {
            string key, value;
            cout << "Enter key: ";
            getline(cin, key);
            cout << "Enter value to add: ";
            getline(cin, value);

			typesManager->addToHyperLogLog(key, value);
            break;
        }
        case 4: {
            string key;
            cout << "Enter key to estimate: ";
            getline(cin, key);

			typesManager->estimateHyperLogLog(key);
            break;
        }
        case 5: return;
        default: cout << "Invalid choice.\n";
        }
    } while (choice != 5);
}

void TypesMenu::showSimHashMenu() {
    cout << "\n========== SimHash ==========\n";
    cout << "1. SH Add Fingerprint\n";
    cout << "2. SH Delete Fingerprint\n";
    cout << "3. SH Get Hamming Distance\n";
    cout << "4. Back\n";
    cout << "=============================\n";
}

void TypesMenu::handleSimHash() {
    int choice;
    do {
        showSimHashMenu();
		choice = getIntInput("Enter choice: ");

        switch (choice) {
        case 1: {
            string key, text;
            cout << "Enter key: ";
            getline(cin, key);
            cout << "Enter text to add: ";
            getline(cin, text);

            typesManager->addSimHashFingerprint(key, text);
            break;
        }
        case 2: {
            string key;
            cout << "Enter key to delete: ";
            getline(cin, key);

            typesManager->deleteSimHashFingerprint(key);
            break;
        }
        case 3: {
            string key1, key2;
            cout << "Enter first key: ";
            getline(cin, key1);
            cout << "Enter second key: ";
            getline(cin, key2);

            typesManager->getHammingDistance(key1, key2);
            break;
        }
        case 4: return;
        default: cout << "Invalid choice.\n";
        }
        } while (choice != 4);
}