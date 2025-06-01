#pragma once
#include <iostream>
#include <string>
#include "System.h"

class MainApp {

private:
	System* system;

public:

	MainApp();
	~MainApp();

	void run();
	void showMenu();
	void handlePut();
	void handleDelete();
	// void handleGet();
};