#include <iostream>
#include <chrono>

#include "wal.h"
using namespace std;


int main(){
	Wal w1;
	
	w1.put("dabar", "100");
	w1.put("pcela ogromna me ujela", "ne znam kako 12345 nije deljivo sa 09434");
	w1.put("bumbar", "200");

	return 0;
}

