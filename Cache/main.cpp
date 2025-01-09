#include "cache.h"
#include <iostream>
using namespace std;
Cache c;

void nadji(pair<int, string> k) {
	Block b;
	bool ima;
	ima = c.get_block(k, b);
	if (ima) {
		cout << "Ima blok: " << b.key.first << " " << b.key.second << endl;
	}
	else {
		cout << "Nema blok\n";
	}
}
void testing() {
	Block b1(make_pair(1, "a"));
	Block b2(make_pair(2, "b"));
	Block b3(make_pair(3, "c"));
	Block b4(make_pair(4, "ffa"));
	Block b5(make_pair(5, "egh"));

	c.add_block(b1);
	
	c.add_block(b2);
	c.add_block(b2);
	
	c.add_block(b1);


	c.add_block(b3);
	c.add_block(b4);
	c.add_block(b5);

	//c.add_node(new Node(b2));

	c.print_cache();

	
}

int main() {
	testing();
	nadji(make_pair(1, "a"));
	nadji(make_pair(2, "b"));
	nadji(make_pair(3, "c"));
	nadji(make_pair(4, "ffa"));
	nadji(make_pair(5, "egh"));
	cout << "Cache ok\n";
	return 0;
}