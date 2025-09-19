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
    cout << "Enter value: ";
    getline(cin, value);
    system->put(key, value);
    cout << "[PUT] Inserted key: " << key << "\n";
}

void MainApp::handleDelete() {
    cout << "Enter key to delete: ";
    getline(cin, key);
    system->del(key);
    cout << "[DELETE] Marked as deleted: " << key << "\n";
}

void MainApp::handleGet() {
    cout << "Enter key to get: ";
    getline(cin, key);
    auto value = system->get(key);
    
    if (value == nullopt) {
        cout << "[GET] Key " << key << " doesnt exists\n";
    }
    else {
        cout << "[GET] Key " << "\033[31m" << key << "\033[0m" << " Value " << "\033[31m" << value.value() << "\033[0m" << "\n";
    }
}

void MainApp::test_case() {
	cout << "\n\n\n[TEST CASE] Inserting 6 key-value pairs...\n";
    key = "key1";
    value = "value1";
    system->put(key, value);
    cout << "[PUT] Inserted key: " << key << "\n";

    key = "key2";
    value = "value2";
    system->put(key, value);
    cout << "[PUT] Inserted key: " << key << "\n";

    key = "key3";
    value = "value3";
    system->put(key, value);
    cout << "[PUT] Inserted key: " << key << "\n";

    key = "key4";
    value = "value4";
    system->put(key, value);
    cout << "[PUT] Inserted key: " << key << "\n";

    key = "key5";
    value = "value5";
    system->put(key, value);
    cout << "[PUT] Inserted key: " << key << "\n";

    key = "key6";
    value = "value6";
    system->put(key, value);
    cout << "[PUT] Inserted key: " << key << "\n";

    key = "key7";
    value = "value7";
    system->put(key, value);
    cout << "[PUT] Inserted key: " << key << "\n";

    key = "key8";
    value = "value8";
    system->put(key, value);
    cout << "[PUT] Inserted key: " << key << "\n";

    key = "key9";
    value = "value9";
    system->put(key, value);
    cout << "[PUT] Inserted key: " << key << "\n";

   
    //TODO: Ovaj key se ne ubaci
    //znam zasto. Zbog tocken bucketa (MISLIM). Bukvalno prepise tocken bucket preko ovog kljuca, i on ne stane
    key = "key11";
    value = "value11";
    system->put(key, value);
    cout << "[PUT] Inserted key: " << key << "\n";

    key = "key10";
    value = "value10";
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
