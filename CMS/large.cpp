#include "cms.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <map>
#include <algorithm>
#include <iomanip>

using namespace std;

void extractDataFromCSV(const string& filename, vector<string>& zip_codes, vector<string>& states) {
    ifstream file(filename);
    string line;
    if (!file.is_open()) {
        cout << "Greska: Datoteka " << filename << " ne moze biti otvorena!" << endl;
        return;
    }
    getline(file, line);
    while (getline(file, line)) {
        stringstream ss(line);
        string temp, zip_code, state;
        getline(ss, temp, ',');        // customer_id
        getline(ss, temp, ',');        // customer_unique_id
        getline(ss, zip_code, ',');    // customer_zip_code_prefix
        getline(ss, temp, ',');        // customer_city
        getline(ss, state, '\n');      // customer_state
        zip_codes.push_back(zip_code);
        states.push_back(state);
    }
    file.close();
}

void printTopN(const CountMinSketch& cms, const vector<string>& elements,
    const string& category, int n = 10) {
    map<string, int> freq_map;
    for (const auto& elem : elements) {
        freq_map[elem] = cms.query(elem);
    }
    vector<pair<string, int>> freq_vec(freq_map.begin(), freq_map.end());
    sort(freq_vec.begin(), freq_vec.end(),
        [](const auto& a, const auto& b) { return a.second > b.second; });

    cout << "\nNajcescih " << n << " " << category << ":\n";
    cout << setw(20) << "Element" << setw(15) << "Ucestalost" << endl;
    cout << string(35, '-') << endl;
    for (int i = 0; i < min(n, (int)freq_vec.size()); ++i) {
        cout << setw(20) << freq_vec[i].first
            << setw(15) << freq_vec[i].second << endl;
    }
}

void testCMS() {
    try {
        CountMinSketch cms_zip_codes(0.01, 0.01);   // Za postanske brojeve
        CountMinSketch cms_states(0.01, 0.01);      // Za države
        vector<string> zip_codes;
        vector<string> states;
        string filename = "../../test_data/olist_customers_dataset.csv";

        extractDataFromCSV(filename, zip_codes, states);
        cout << "Dodavanje podataka u Count-Min Sketch..." << endl;

        for (const auto& zip : zip_codes) {
            cms_zip_codes.add(zip);
        }
        for (const auto& state : states) {
            cms_states.add(state);
        }

        printTopN(cms_zip_codes, zip_codes, "postanskih brojeva", 10);
        printTopN(cms_states, states, "drzava", 27);

        cout << "\nTestiranje serijalizacije za drzave..." << endl;
        auto data = cms_states.serialize();

        auto deserialized = CountMinSketch::deserialize(data);
        cout << "\nProvera nakon deserijalizacije:" << endl;

        for (const auto& state : states) {
            if (cms_states.query(state) != deserialized.query(state)) {
                cout << "Greska serijalizacije za drzavu: " << state << endl;
                return;
            }
        }
        cout << "Provera serijalizacije uspjesno zavrsena." << endl;
    }
    catch (const exception& e) {
        cout << "Greska: " << e.what() << endl;
    }
}

int main() {
    testCMS();
    return 0;
}