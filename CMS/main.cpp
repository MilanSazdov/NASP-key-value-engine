#include "cms.h"
#include <iostream>
#include <iomanip>
#include <string>
#include <vector>
#include <fstream>

using namespace std;

void printResults(const CountMinSketch& cms, const vector<string>& elements) {
    cout << "\nFrekvencije elemenata:\n";
    cout << setw(15) << "Element" << setw(12) << "Frekvencija" << endl;
    cout << string(30, '-') << endl;

    for (const auto& elem : elements) {
        cout << setw(15) << elem << setw(12) << cms.query(elem) << endl;
    }
}

void testSerialization(const CountMinSketch& cms) {
    auto data = cms.serialize();

    auto deserialized = CountMinSketch::deserialize(data);

    cout << "\nTestiranje nakon serijalizacije/deserijalizacije:\n";
    vector<string> testElements = { "Ana", "Tijana", "Mina" };
    for (const auto& elem : testElements) {
        cout << "Frekvencija za " << elem << ": " << deserialized.query(elem) << endl;
    }
}

int mainl() {
    CountMinSketch cms(0.01, 0.01);

    vector<string> elements;

    vector<string> imena = {
        "Ana", "Marko", "Jelena", "Nikola", "Maja",
        "Stefan", "Marina", "Petar", "Ivana", "Luka",
        "Milica", "Aleksandar", "Jovana", "Dragan", "Tijana",
        "Vladimir", "Tamara", "Nemanja", "Katarina", "Dusan"
    };

    for (int i = 0; i < 3; ++i) {
        for (const auto& ime : imena) {
            cms.add(ime);
            if (i == 0) elements.push_back(ime);
        }
    }

    vector<string> cestaImena = { "Ana", "Marko", "Jelena", "Maja", "Stefan" };
    for (int i = 0; i < 5; ++i) {
        for (const auto& ime : cestaImena) {
            cms.add(ime);
        }
    }

    vector<string> novaImena = {
        "Filip", "Sara", "Ognjen", "Nina", "Branko",
        "Teodora", "Veljko", "Sofija", "Lazar", "Mila",
        "Bogdan", "Elena", "Vuk", "Tara", "Pavle",
        "Mina", "Mateja", "Nadja", "Vasilije", "Jana"
    };

    for (const auto& ime : novaImena) {
        cms.add(ime);
        elements.push_back(ime);
    }

    printResults(cms, elements);

    testSerialization(cms);

    return 0;
}