#pragma once
#include <iostream>
#include <string>
#include "../System/System.h"
#include "TypesMenu.h"

class MainApp {

// TODO: verovatno bi ovde (u ovaj projekat) bilo najbolje dodati Config klasu, pa je pozvati pre kreiranja system klase, i proslediti joj
private:
	System* system;
	TypesMenu* typesMenu;

	void debugWal();
	void debugMemtable();

public:
	void test_case();	//just for testing

	MainApp();
	~MainApp();

	void test_leveled();
	void run();
	void showMenu();
	void handlePut();
	void handleDelete();
	void handleGet();
	void handlePrefixScan();
	void handleRangeScan();
	void handleValidate();
};