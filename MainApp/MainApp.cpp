#include "MainApp.h"
#include <limits>

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
}

static int getIntInput(const string& prompt) {
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

void MainApp::test_leveled() {
    for (int i = 1; i <= 10; i++) {
        string key = "user" + (i < 10 ? "00" + to_string(i) : (i < 100 ? "0" + to_string(i) : to_string(i)));
        string value = "data" + to_string(i % 5); // ponavljanje vrednosti da pravi duplikate 
        system->put(key, value);
        cout << "[PUT] Inserted key: " << key << " => " << value << "\n";
    }
	system->del("user005");
    system->del("user007");
    system->del("user009");

    cout << "\n\n\n[TEST] Inserted 10 users, deleted 3 (user005, user007, user009)\n";
    cout << "[TEST] Now inserting more users to trigger compaction...\n";
    for (int i = 11; i <= 30; i++) {
        string key = "user" + (i < 10 ? "00" + to_string(i) : (i < 100 ? "0" + to_string(i) : to_string(i)));
        string value = "data" + to_string(i % 5); // ponavljanje vrednosti da pravi duplikate 
        system->put(key, value);
        cout << "[PUT] Inserted key: " << key << " => " << value << "\n";
    }
    cout << "[TEST] Inserted 20 more users (user011 to user030)\n";
	cout << "[TEST] This should have triggered compaction if memtable limit was reached.\n\n\n";


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
    test_leveled();
    
	
    //system->removeSSTables();

    while (true) {
        showMenu();
        
		choice = getIntInput("Enter choice: ");
        
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
            cout << "Invalid choice." << endl;
            break;
        }
        cout << endl;
    }
}
