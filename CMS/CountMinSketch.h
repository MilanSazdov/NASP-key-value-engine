#ifndef COUNT_MIN_SKETCH_H
#define COUNT_MIN_SKETCH_H

#include <vector>
#include <string>
#include <functional>
#include <ctime>

using namespace std;

class CountMinSketch {
private:
	unsigned int m; //Velicina seta
	unsigned int k; //Broj hash funkcija
	double errorRate; //Preciznost
	double probability; //Tacnost
	vector<vector<int>> matrix; //Tabela

	vector<function<size_t(const string&)>> hashFunctions; // Hash functions
	unsigned int timeConst; // Seed for generating hash functions
	size_t h2_seed; // Seed for the second hash function 


public:
	CountMinSketch(double errorRate, double probability);

	void insert(const string& element);

	int estimate(const string& element) const;

	unsigned int calculateSizeOfSet(double errorRate);

	unsigned int calculateNumberOfHashFunctions(double probability);

	//Serijalizacija

	//Deserijalizacija
};

#endif
