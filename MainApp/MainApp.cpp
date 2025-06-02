#include "MainApp.h"

MainApp::MainApp() {
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

void MainApp::handlePut() {
    std::string key, value;
    std::cin.ignore();
    std::cout << "Enter key: ";
    std::getline(std::cin, key);
    std::cout << "Enter value: ";
    std::getline(std::cin, value);
    system->put(key, value, false);
    std::cout << "[PUT] Inserted key: " << key << "\n";
}

void MainApp::handleDelete() {
    std::string key;
    std::cin.ignore();
    std::cout << "Enter key to delete: ";
    std::getline(std::cin, key);
    system->put(key, "", true);
    std::cout << "[DELETE] Marked as deleted: " << key << "\n";
}

void MainApp::run() {
    int choice;

    while (true) {
        showMenu();
        std::cin >> choice;

        switch (choice) {
        case 1: handlePut(); break;
        case 2: handleDelete(); break;
        case 3:
            std::cout << "[GET] Not implemented yet.\n";
            break;
        case 4:
            std::cout << "Exiting...\n";
            return;
        case 404:
            debugWal();
            debugMemtable();
            break;
        default:
            std::cout << "Invalid choice.\n";
        }
    }
}
