#include "hll.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>

using namespace std;

void extractDataFromCSV(const string& filename, vector<string>& customer_ids, vector<string>& zip_codes, vector<string>& states) {
    ifstream file(filename);
    string line;

    if (!file.is_open()) {
        cout << "Greška: Fajl " << filename << " nije mogao da se otvori!" << endl;
        return; 
    }

    getline(file, line);

    while (getline(file, line)) {
        stringstream ss(line);
        string id, zip_code, state, temp;

        getline(ss, id, ','); // customer_id
        getline(ss, temp, ','); // customer_unique_id (ne koristimo)
        getline(ss, zip_code, ','); // customer_zip_code_prefix
        getline(ss, temp, ','); // customer_city
        getline(ss, state, '\n'); // customer_state

        customer_ids.push_back(id);
        zip_codes.push_back(zip_code);
        states.push_back(state);
    }

    file.close();
}

void addTestData(HyperLogLog& hll) {
    hll.add("apple");
    hll.add("dog");
    hll.add("rose");
    hll.add("carrot");
    hll.add("oak");
    hll.add("sparrow");
    hll.add("shark");
    hll.add("iron");
    hll.add("mercury");
    hll.add("violin");
    hll.add("blue");
    hll.add("python");
    hll.add("java");
    hll.add("winter");
    hll.add("happiness");
    hll.add("oxygen");
    hll.add("computer");
    hll.add("earth");
    hll.add("gold");
    hll.add("diamond");
    hll.add("river");
    hll.add("mountain");
    hll.add("sun");
    hll.add("moon");
    hll.add("paper");
    hll.add("pen");
    hll.add("book");
    hll.add("courage");
    hll.add("freedom");
    hll.add("justice");
}

void printEstimate(const HyperLogLog& hll) {
    double estimate = hll.estimate();
    cout << estimate << endl;
}

// Testiranje HyperLogLog sa CSV podacima
void testHLL() {
    try {
        HyperLogLog hll_user_ids(14);  // p=14 je dobar izbor za ~100,000 elemenata
        HyperLogLog hll_zip_codes(14); // p=14 za zip kodove
        HyperLogLog hll_states(14);    // p=14 za states

        vector<string> customer_ids;
        vector<string> zip_codes;
        vector<string> states;
        string filename = "../../data/olist_customers_dataset.csv";//https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
        extractDataFromCSV(filename, customer_ids, zip_codes, states);

        for (const auto& id : customer_ids) {
            hll_user_ids.add(id);  // Dodaj customer_id u HLL
        }

        for (const auto& zip : zip_codes) {
            hll_zip_codes.add(zip);  // Dodaj zip kod u HLL
        }

        for (const auto& state : states) {
            hll_states.add(state);  // Dodaj state u HLL
        }

        cout << "Procena broja razlicitih korisnika: ";
        printEstimate(hll_user_ids);

        cout << "Procena broja razlicitih zip kodova: ";
        printEstimate(hll_zip_codes);

        cout << "Procena broja razlicitih drzava: ";
        printEstimate(hll_states);
       
    }
    catch (const std::exception& e) {
        cout << "Greska: " << e.what() << endl;
    }
}

int mainc() {
    testHLL();
    return 0;
}
