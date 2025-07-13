#include "TypesMenu.h"
#include <iostream>

using namespace std;

TypesMenu::TypesMenu(System* sys) {
	system = sys;
    if (!system) {
        throw invalid_argument("System pointer cannot be null");
	} else{
        typesManager = system->getTypesManager();
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
        cout << "Enter choice: ";
        cin >> choice;
        cin.ignore();

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
        cout << "Enter choice: ";
        cin >> choice;
        cin.ignore();

        switch (choice) {
        case 1: {
            string key;
            int n;
            double p;

            cout << "Enter key: ";
            getline(cin, key);

            // TODO: Check for int values
            cout << "Enter number of elements (n): ";
            cin >> n;

            cout << "Enter false positivity probability (p): ";
            cin >> p;
            cin.ignore(); // To consume the newline

			typesManager->createBloomFilter(key, n, p);
            cout << "Bloom Filter created with key: " << key
                << ", n: " << n << ", p: " << p << endl;
            break;
        }
        case 2: {
            string key;
            cout << "Enter key to delete: ";
            getline(cin, key);

			typesManager->deleteBloomFilter(key);
            cout << "Bloom Filter with key: " << key << " deleted" << endl;
            break;
        }
        case 3: {
            string key, value;
            cout << "Enter key: ";
            getline(cin, key);
            cout << "Enter value to add: ";
            getline(cin, value);

			typesManager->addToBloomFilter(key, value);
			cout << "Value '" << value << "' added to Bloom Filter '" << key << "'.\n";
            break;
        }
        case 4: {
            string key, value;
            cout << "Enter key: ";
            getline(cin, key);
            cout << "Enter value to check: ";
            getline(cin, value);

			bool exist =  typesManager->hasKeyInBloomFilter(key, value);
            cout << "Value '" << value << "' is "
				<< (exist ? "present" : "not present") << " in Bloom Filter '" << key << "'.\n";
            break;
        }
        case 5: return;
        default: cout << "Invalid choice.\n";
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
        cout << "Enter choice: ";
        cin >> choice;
        cin.ignore();
        switch (choice) {
        case 1:
        {
            string key;
            double epsilon, delta;
            cout << "Enter key: ";
            getline(cin, key);
            cout << "Enter epsilon: ";
            cin >> epsilon;
            cout << "Enter delta: ";
            cin >> delta;
            cin.ignore(); 


            typesManager->createCountMinSketch(key, epsilon, delta);
            cout << "Count-Min Sketch created with key: " << key
                << ", epsilon: " << epsilon << ", delta: " << delta << endl;
            break;
        }
        case 2: {
            string key;
            cout << "Enter key to delete: ";
            getline(cin, key);

			typesManager->deleteCountMinSketch(key);
            cout << "Count-Min Sketch with key: " << key << " deleted" << endl;
            break;
        }
        case 3: {
            string key, value;
            cout << "Enter key: ";
            getline(cin, key);
            cout << "Enter value to add: ";
            getline(cin, value);

			typesManager->addToCountMinSketch(key, value);
			cout << "Value '" << value << "' added to Count-Min Sketch '" << key << "'.\n";
            break;
        }
        case 4: {
            string key, value;
            cout << "Enter key: ";
            getline(cin, key);
            cout << "Enter value to get: ";
            getline(cin, value);

			auto frequency = typesManager->getFromCountMinSketch(key, value);
            if (frequency.has_value()) {
                cout << "Estimated frequency of '" << value << "' in Count-Min Sketch '" << key << "': "
                     << frequency.value() << endl;
            } else {
                cout << "Value '" << value << "' not found in Count-Min Sketch '" << key << "'.\n";
			}
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
        cout << "Enter choice: ";
        cin >> choice;
        cin.ignore();
        switch (choice) {
        case 1: {
            string key;
            int p;
            cout << "Enter key: ";
            getline(cin, key);
            cout << "Enter precision (p): ";
            cin >> p;
            cin.ignore(); // To consume the newline

			typesManager->createHyperLogLog(key, p);
            cout << "HyperLogLog created with key: " << key << ", p: " << p << endl;
            break;
        }
        case 2: {
            string key;
            cout << "Enter key to delete: ";
            getline(cin, key);

			typesManager->deleteHyperLogLog(key);
            cout << "HyperLogLog with key: " << key << " deleted" << endl;

            break;
        }
        case 3: {
            string key, value;
            cout << "Enter key: ";
            getline(cin, key);
            cout << "Enter value to add: ";
            getline(cin, value);

			typesManager->addToHyperLogLog(key, value);
			cout << "Value '" << value << "' added to HyperLogLog '" << key << "'.\n";
            break;
        }
        case 4: {
            string key;
            cout << "Enter key to estimate: ";
            getline(cin, key);

			auto cardinality = typesManager->estimateHyperLogLog(key);
            if (cardinality.has_value()) {
                cout << "Estimated cardinality of HyperLogLog '" << key << "': "
                     << cardinality.value() << endl;
            } else {
                cout << "HyperLogLog with key '" << key << "' does not exist.\n";
			}

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
        cout << "Enter choice: ";
        cin >> choice;
        cin.ignore();
        switch (choice) {
        case 1: {
            string key, text;
            cout << "Enter key: ";
            getline(cin, key);
            cout << "Enter text to add: ";
            getline(cin, text);

            typesManager->addSimHashFingerprint(key, text);
            cout << "Fingerprint for key '" << key << "' added successfully.\n";
            break;
        }
        case 2: {
            string key;
            cout << "Enter key to delete: ";
            getline(cin, key);

            typesManager->deleteSimHashFingerprint(key);
            cout << "Fingerprint with key: " << key << " deleted" << endl;
            break;
        }
        case 3: {
            string key1, key2;
            cout << "Enter first key: ";
            getline(cin, key1);
            cout << "Enter second key: ";
            getline(cin, key2);

            auto distance = typesManager->getHammingDistance(key1, key2);
            if (distance.has_value()) {
                cout << "Hamming distance between '" << key1 << "' and '" << key2 << "': "
                    << distance.value() << endl;
            }
            else {
                cout << "One or both fingerprints do not exist.\n";
                break;
            }

            break;
        }
        case 4: return;
        default: cout << "Invalid choice.\n";
        }
        } while (choice != 4);
}