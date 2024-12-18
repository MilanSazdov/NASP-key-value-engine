#include "CountMinSketch.h"
#include <functional>
#include <cmath>
#include <random>

CountMinSketch::CountMinSketch(double errorRate, double probability) {
	m = calculateSizeOfSet(errorRate);
	k = calculateNumberOfHashFunctions(probability);
	matrix = vector<vector<int>>(k, vector<int>(m, 0));

	timeConst = static_cast<unsigned int>(time(nullptr));
	mt19937 rng(timeConst);
	uniform_int_distribution<size_t> dist(0, numeric_limits<size_t>::max());
	this->h2_seed = dist(rng);
	hashFunctions.reserve(k);
	for (unsigned int i = 0; i < k; ++i) {
		hashFunctions.emplace_back(
			[i, this](const std::string& key) {
				size_t h1 = std::hash<std::string>()(key);
				size_t h2 = std::hash<std::string>()(std::to_string(h2_seed) + key);
				return (h1 + i * h2) % this->m;
			}
		);
	}


};

void CountMinSketch::insert(const string& element) {
	for (unsigned int i = 0; i < k; ++i) {
		size_t index = hashFunctions[i](element) % m;
		matrix[i][index] += 1;
	}
}

int CountMinSketch::estimate(const string& element) const {
	vector<int> tableValues;
	for (unsigned int i = 0; i < k; ++i) {
		size_t index = hashFunctions[i](element) % m;
		if (!matrix[i][index]) {
			return 0;
		}
		tableValues.push_back(matrix[i][index]);
	}
	auto minElement = min_element(tableValues.begin(), tableValues.end());
	return *minElement;
}


unsigned int CountMinSketch::calculateSizeOfSet(double errorRate) {
	return static_cast<unsigned int>(ceil(exp(1.0) / errorRate));
}

unsigned int CountMinSketch::calculateNumberOfHashFunctions(double probability) {
	return static_cast<unsigned int>(ceil(log(1 / probability)));
}