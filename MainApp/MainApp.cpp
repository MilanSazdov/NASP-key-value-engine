#include "MainApp.h"
using namespace std;

MainApp::MainApp() {
    std::cout << "[MainApp] Initializing Key-Value Engine...\n";
    system = new System();
    typesMenu = new TypesMenu(system);
}

MainApp::~MainApp() {
    delete system;
    delete typesMenu;
}

void MainApp::debugWal() {
    std::cout << "[DEBUG] Printing WAL contents...\n";
    system->debugWal();
}

void MainApp::debugMemtable() {
    std::cout << "[DEBUG] Printing Memtable contents...\n";
    system->debugMemtable();
}

void MainApp::showMenu() {
    std::cout << "\n========== Key-Value Engine ==========\n";
    std::cout << "1. PUT(key, value)   - Add or update a key-value pair\n";
    std::cout << "2. DELETE(key)       - Mark key as deleted\n";
    std::cout << "3. GET(key)          - (Not implemented yet)\n";
    std::cout << "4. TYPES             - Types menu\n";
    std::cout << "5. PREFIX SCAN       - Scan for keys starting with prefix\n";
    std::cout << "6. RANGE SCAN        - Scan for keys in a range\n";
    std::cout << "7. VALIDATE SSTABLES - Validate data integrity for a level\n";
    std::cout << "8. EXIT              - Exit program\n";
    std::cout << "======================================\n";
    std::cout << "Enter choice: ";
}

string key, value;
void MainApp::handlePut() {
    cout << "Enter key: ";
    getline(cin, key);
    if (key.rfind("system_", 0) == 0) {
        cout << "\033[31m[SYSTEM ERROR] Attempt to use reserved system key. Operation aborted.\n\033[0m";
        return;
    }
    cout << "Enter value: ";
    getline(cin, value);
    system->put(key, value);
    cout << "[PUT] Inserted key: " << key << "\n";
}

void MainApp::handleDelete() {
    cout << "Enter key to delete: ";
    getline(cin, key);
    if (key.rfind("system_", 0) == 0) {
        cout << "\033[31m[SYSTEM ERROR] Attempt to use reserved system key. Operation aborted.\n\033[0m";
        return;
    }
    system->del(key);
    cout << "[DELETE] Marked as deleted: " << key << "\n";
}

void MainApp::handleGet() {
    cout << "Enter key to get: ";
    getline(cin, key);
    if (key.rfind("system_", 0) == 0) {
        cout << "\033[31m[SYSTEM ERROR] Attempt to use reserved system key. Operation aborted.\n\033[0m";
        return;
    }
    auto value = system->get(key);
    
    if (value == nullopt) {
        cout << "[GET] Key " << "\033[31m" << key << "\033[0m" << " doesnt exists\n";
    }
    else {
        cout << "[GET] Key " << "\033[31m" << key << "\033[0m" << " Value " << "\033[31m" << value.value() << "\033[0m" << "\n";
    }
}

void MainApp::handlePrefixScan() {
    cout << "Enter prefix: ";
    std::string prefix;
    getline(cin, prefix);
    cout << "Enter page size: ";
    int page_size;
    cin >> page_size;
    cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

    system->prefixScan(prefix, page_size);
}

void MainApp::handleRangeScan() {
    cout << "Enter min key: ";
    std::string min_key;
    getline(cin, min_key);

    cout << "Enter max key: ";
    std::string max_key;
    getline(cin, max_key);

    cout << "Enter page size: ";
    int page_size;
    cin >> page_size;
    cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

    system->rangeScan(min_key, max_key, page_size);
}

void MainApp::handleValidate() {
    int level;
    cout << "Enter level to validate: ";
    cin >> level;

    // Provera da li je unos validan broj
    if (cin.fail()) {
        cout << "\033[31m[ERROR] Invalid input. Please enter a number.\n\033[0m";
        cin.clear(); // Čisti flag greške
        cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n'); // Odbacuje loš unos
        return;
    }

    cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n'); // Čisti bafer

    // Pozivamo novu funkciju iz System klase
    system->validateSSTables(level);
}

void MainApp::test_case() {
    int n = 100;
	cout << "\n\n\n[TEST CASE] Inserting "<< n <<" key-value pairs...\n";
    for(int i = 1; i <= n; i++) {
        key = "key" + to_string(i);
        value = "value" + to_string(i);
        system->put(key, value);
        cout << "[PUT] Inserted key: " << key << "\n";
	}
    
	// cout << "\n\n\n\n\n[TEST CASE] Retrieving 6 keys (key1 to key9)...\n";
    // for (int i = 1; i <= n; i++) {
    //     key = "key" + to_string(i);
	// 	cout << "Getting key: " << key << "\n";
    //     auto value = system->get(key);
    //     if (value == nullopt) {
    //         cout << "[GET] Key " << "\033[31m" << key << "\033[0m" << " doesnt exists\n";
    //     }
    //     else {
    //         cout << "[GET] Key " << "\033[31m" << key << "\033[0m" << " Value " << "\033[31m" << value.value() << "\033[0m" << "\n";
	// 	}
    // }

    key = "key7";
    value = "value7";
    system->put(key, value);
    cout << "[PUT] Inserted key: " << key << "\n";

    key = "key10";
    value = "value10";
    system->put(key, value);
    cout << "[PUT] Inserted key: " << key << "\n";

    key = "key11";
    value = "value11";
    system->put(key, value);
    cout << "[PUT] Inserted key: " << key << "\n";

    key = "key12";
    value = "value12";
    system->put(key, value);
    cout << "[PUT] Inserted key: " << key << "\n";

    key = "key13";
    value = "value13";
    system->put(key, value);
    cout << "[PUT] Inserted key: " << key << "\n";

    cout << "[TEST CASE] Done...\n\n\n\n";
}


void MainApp::run() {
    int choice;
    //cin.ignore();
    test_case();
    
	
    //system->removeSSTables();

    while (true) {
        showMenu();
        cin >> choice;
        cin.ignore();

        switch (choice) {
        case 1: handlePut(); break;
        case 2: handleDelete(); break;
        case 3: handleGet(); break;
        case 4: typesMenu->showMenu(); break;
        case 5: handlePrefixScan(); break;
        case 6: handleRangeScan(); break;
        case 7: handleValidate(); break;
        case 8:
            cout << "Exiting...\n";
            return;
        case 404:
            debugWal();
            debugMemtable();
            break;
        default:
            cout << "Invalid choice.\n";
        }
        cout << endl;
    }
}
