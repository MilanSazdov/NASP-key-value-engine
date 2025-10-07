#include "BloomFilter.h"
#include <iostream>
#include <iomanip>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>

using namespace std;

void ispisiRezultatTesta(const string& ime, bool jePronadjen, bool trebaDaPostoji) {
    cout << setw(20) << ime << " " << setw(15);
    if (jePronadjen && trebaDaPostoji) {
        cout << "Tacno (Prisutan)";
    }
    else if (!jePronadjen && !trebaDaPostoji) {
        cout << "Tacno (Odsutan)";
    }
    else if (jePronadjen && !trebaDaPostoji) {
        cout << "Lazno pozitivan!";
    }
    else {
        cout << "Lazno negativan!";
    }
    cout << endl;
}

void testirajBloomFilter() {
    // Kreiraj Bloom filter za ocekivano 20 elemenata sa 5% laznih pozitivnih
    BloomFilter bf(20, 0.05);

    // Elementi koje cemo dodati u filter
    vector<string> dodatiElementi = {
        "Ana", "Marko", "Jelena", "Nikola", "Maja",
        "Stefan", "Marina", "Petar", "Ivana", "Luka"
    };

    // Elementi koje necemo dodati (testiramo lazne pozitive)
    vector<string> nedodatiElementi = {
        "Anja", "Marija", "Jovan", "Nina", "Milan",
        "Stefania", "Marin", "Pera", "Iva", "Lukas",
        "Bogdan", "Elena", "Vuk", "Tara", "Pavle"
    };

    // Dodaj elemente u filter
    for (const auto& elem : dodatiElementi) {
        bf.add(elem);
    }

    cout << "===== Testiranje dodanih elemenata (treba da budu prisutni) =====\n";
    for (const auto& elem : dodatiElementi) {
        bool jePronadjen = bf.possiblyContains(elem);
        ispisiRezultatTesta(elem, jePronadjen, true);
    }

    cout << "\n===== Testiranje nedodanih elemenata (treba da budu odsutni) =====\n";
    int laznihPozitivnih = 0;
    for (const auto& elem : nedodatiElementi) {
        bool jePronadjen = bf.possiblyContains(elem);
        ispisiRezultatTesta(elem, jePronadjen, false);
        if (jePronadjen) laznihPozitivnih++;
    }

    // Izracunaj stopu laznih pozitivnih
    double stopaLaznihPozitivnih = (double)laznihPozitivnih / nedodatiElementi.size();
    cout << "\nBroj laznih pozitivnih rezultata: " << laznihPozitivnih << " / " << nedodatiElementi.size()
        << " (" << (stopaLaznihPozitivnih * 100) << "%)\n";

    // Test sa slicnim imenima (granicni slucajevi)
    vector<string> slicnaImena = {
        "An", "Mark", "Jelen", "Nikol", "Maj",
        "Stefa", "Marin", "Petr", "Ivan", "Luk"
    };

    cout << "\n===== Testiranje slicnih imena (granicni slucajevi) =====\n";
    for (const auto& elem : slicnaImena) {
        bool jePronadjen = bf.possiblyContains(elem);
        ispisiRezultatTesta(elem, jePronadjen, false);
    }
}

void testirajSerijalizaciju() {
    cout << "\n===== Testiranje serijalizacije/deserijalizacije =====\n";

    BloomFilter original(10, 0.1);
    vector<string> testniElementi = { "Jedan", "Dva", "Tri" };
    for (const auto& elem : testniElementi) {
        original.add(elem);
    }

    // Serijalizuj i deserijalizuj
    auto serijalizovano = original.serialize();
    BloomFilter restaurirani = BloomFilter::deserialize(serijalizovano);

    // Testiraj sve elemente
    for (const auto& elem : testniElementi) {
        bool originalniRezultat = original.possiblyContains(elem);
        bool restauriraniRezultat = restaurirani.possiblyContains(elem);

        cout << "Element: " << setw(10) << elem
            << " | Original: " << (originalniRezultat ? "Prisutan" : "Odsutan")
            << " | Restaurirani: " << (restauriraniRezultat ? "Prisutan" : "Odsutan")
            << " | " << (originalniRezultat == restauriraniRezultat ? "OK" : "NE POKLAPA SE!")
            << endl;
    }

    // Testiraj nedodati element
    string nedodatiElement = "Cetiri";
    bool originalniRezultat = original.possiblyContains(nedodatiElement);
    bool restauriraniRezultat = restaurirani.possiblyContains(nedodatiElement);

    cout << "Element: " << setw(10) << nedodatiElement
        << " | Original: " << (originalniRezultat ? "Prisutan" : "Odsutan")
        << " | Restaurirani: " << (restauriraniRezultat ? "Prisutan" : "Odsutan")
        << " | " << (originalniRezultat == restauriraniRezultat ? "OK" : "NE POKLAPA SE!")
        << endl;
}

int main() {
    cout << "===== Testiranje Bloom filtera =====\n";
    testirajBloomFilter();
    testirajSerijalizaciju();
    return 0;
}