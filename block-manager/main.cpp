#include "block-manager.h"

vector<byte> v;

int main() {
	Block_manager bm;

	v.push_back((byte)65);
	v.push_back((byte)66);
	v.push_back((byte)67);

	bool error;
	composite_key key = make_pair(0, "file.bin");

	bm.write_block(key, v);

	v.clear();
	v = bm.read_block(key, error);

	for (byte b : v) {
		cout << (char)b;
	}
	cout << endl;

	return 0;
}