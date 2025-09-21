#include "MainApp.h"
using namespace std;

MainApp::MainApp() {
    std::cout << "[MainApp] Initializing Key-Value Engine...\n";
    system = new System();
    typesMenu = new TypesMenu(system);
}

MainApp::~MainApp() {
    delete system;
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
    std::cout << "5. EXIT              - Exit program\n";
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

void MainApp::test_case() {
    int n = 6;
	cout << "\n\n\n[TEST CASE] Inserting 9 key-value pairs...\n";
    for(int i = 1; i <= n; i++) {
        key = "key" + to_string(i);
        value = "value" + to_string(i);
        system->put(key, value);
        cout << "[PUT] Inserted key: " << key << "\n";
	}
    
	cout << "\n\n\n\n\n[TEST CASE] Retrieving 9 keys (key1 to key9)...\n";
    for (int i = 1; i <= n; i++) {
        key = "key" + to_string(i);
		cout << "Getting key: " << key << "\n";
        auto value = system->get(key);
        if (value == nullopt) {
            cout << "[GET] Key " << "\033[31m" << key << "\033[0m" << " doesnt exists\n";
        }
        else {
            cout << "[GET] Key " << "\033[31m" << key << "\033[0m" << " Value " << "\033[31m" << value.value() << "\033[0m" << "\n";
		}
    }

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

    system->sstCursor->read_tables();

    bool end = false;
    while(!end) {
        system->prefixScan("key", 5, end);
    }

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
        case 5:
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
