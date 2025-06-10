#include "block-manager.h"
#include <iostream>

using namespace std;
const string file_name = "wal_001.log";

void testing() {
	Block_manager bm;
	Block b;

	vector<char> attempt;

	attempt.push_back('a');
	attempt.push_back('b');
	attempt.push_back('c');

	bm.write_data(file_name, attempt);

	attempt[0] = '1';
	attempt[1] = '2';
	attempt[2] = '3';
	attempt.push_back('4');
	attempt.push_back('5');
	bm.write_data(file_name, attempt);

	bool error;
	vector<char> data;

	data = bm.read_data(file_name);

	for (char c : data) {
		cout << c;
	}
	cout << endl;
	cout << "testiranje block-managera";
}
int main() {
	testing();

	return 0;
}