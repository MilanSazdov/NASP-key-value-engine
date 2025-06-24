#pragma once
#include <iostream>
#include <string>
#include "System.h"

class MainApp {

// TODO: verovatno bi ovde (u ovaj projekat) bilo najbolje dodati Config klasu, pa je pozvati pre kreiranja system klase, i proslediti joj
private:
	System* system;

	void debugWal();
	void debugMemtable();

public:

	MainApp();
	~MainApp();

	void run();
	void showMenu();
	void handlePut();
	void handleDelete();
	void handleGet();
};