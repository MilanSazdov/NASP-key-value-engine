#include "cache.h"
#include <iostream>
# define pb push_back
# define mp make_pair

using namespace std;

typedef pair<int, string> composite_key;
struct pair_hash {
	size_t operator()(const pair<int, string>& p) const {
		return hash<int>()(p.first) ^ hash<string>()(p.second);
	}
};

Cache<string> c;
Cache<composite_key, pair_hash> c2;

void get(string key) {
	cout << "Get " << key << endl;
	vector<byte> val;
	bool err;

	val = c.get(key, err);
	cout << endl;

	if (err) {
		cout << key << " doesnt exists\n";
	}
	else {
		cout << "value: ";
		for (byte b : val) {
			cout << (char)b;
		}
		cout << endl;
	}
}

void get2(composite_key key) {
	cout << "Get (" << key.first << ", " << key.second << ") -> ";
	vector<byte> val;
	bool err;

	val = c2.get(key, err);
	

	if (err) {
		cout << "  doesnt exists\n";
	}
	else {
		
		for (byte b : val) {
			cout << (char)b;
		}
		cout << endl;
	}
	cout << endl;
}
vector<byte> d1 = { (byte)75, (byte)101, (byte)121, (byte)49 };
vector<byte> d2 = { (byte)75, (byte)101, (byte)121, (byte)50 };
vector<byte> d3 = { (byte)75, (byte)101, (byte)121, (byte)51 };
vector<byte> d4 = { (byte)75, (byte)101, (byte)121, (byte)52 };
vector<byte> d5 = { (byte)75, (byte)101, (byte)121, (byte)53 };
vector<byte> d6 = { (byte)75, (byte)101, (byte)121, (byte)54 };
vector<byte> d7 = { (byte)75, (byte)101, (byte)121, (byte)55 };
vector<byte> d8 = { (byte)75, (byte)101, (byte)121, (byte)56 };


void test1() {
	string s1 = "key1";
	string s2 = "key2";
	string s3 = "key3";
	string s4 = "key4";
	string s5 = "key5";
	string s6 = "key6";

	c.put(s1, d1);;

	get(s1);

	c.put(s2, d2);

	get(s2);

	c.put(s3, d3);
	c.put(s4, d4);


	c.put(s5, d5);

	get(s1);
	get(s2);

	c.put(s6, d6);
	get(s3);
}


void test2() {
	composite_key s1 = make_pair(1, "key1");
	composite_key s2 = make_pair(2, "key2");
	composite_key s3 = make_pair(1, "key3");
	composite_key s4 = make_pair(2, "key4");
	composite_key s5 = make_pair(2, "key5");
	composite_key s6 = make_pair(1, "key6");

	c2.put(s1, d1);

	get2(s1);

	c2.put(s2, d2);

	get2(s2);

	c2.put(s3, d3);
	c2.put(s4, d4);


	c2.put(s5, d5);

	get2(s1);
	get2(s2);

	c2.put(s6, d6);
	get2(s3);
}

// This is testing memory managing in cache
void test_memory_leaks() {
	// Change key, and try to put many records in cache. After that, cache should have only last few records in memory
	
	composite_key key = mp(10, "test_string");
	composite_key key_to_find = mp(10, "test_string");

	for (int i = 1; i <= 1000000; i++) {
		c2.put(key, d2);
		key.first++;
		
		// every second tick, we will try to find random key, or key that actually exists
		if (i % 2 == 0) {
			key_to_find.first = rand() % key.first;
		}
		else {
			// looking in last 5 elements
			key_to_find.first = key.first - rand() % 5;
		}
		
		get2(key_to_find);
	}
	cout << "Inputing succeds\n";

	for (int i = 10; i >= 1; i--) {
		get2(key);
		key.first--;
	}
}

// This is testing cache basic functionalities, and LRU algorithm
// test1 and test2 should produce "same" output
void test_logic() {
	test1();
	cout << "\n\n_______________________________________\n";
	test2();
}

// Both test passed. (No bugs I hope :))
int main() {

	// test_logic();
	// test_memory_leaks();


	return 0;
}