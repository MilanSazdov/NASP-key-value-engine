#include "MainApp.h"
using namespace std;

MainApp::MainApp() {
    // TODO: verovatno bi ovde (u ovaj projekat) bilo najbolje dodati Config klasu, pa je pozvati pre kreiranja system klase, i proslediti joj
    // TODO: Config c = new Config()
    // TODO: system = new System(c)   ovako nesto...?
    // TODO: obavezno fajl config.json staviti u root directory!

    std::cout << "[MainApp] Initializing Key-Value Engine...\n";
    system = new System();
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
    std::cout << "4. EXIT              - Exit program\n";
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

}

void MainApp::run() {
    int choice;
    cin.ignore();

    while (true) {
        showMenu();
        cin >> choice;

        switch (choice) {
        case 1: handlePut(); break;
        case 2: handleDelete(); break;
        case 3: handleGet(); break;
        case 4:
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
