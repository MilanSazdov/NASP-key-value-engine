#include <iostream>
#include <chrono>
#include <fstream>
#include <vector>
#include <filesystem> 
#include "block-manager.h"
#include "wal.h"

namespace fs = std::filesystem;
using namespace std;
const string WAL_FOLDER = "../data/wal_logs";

void info(const string s) {
    cout << "[INFO]" + s + "\n";
}

/*
    STA TREBA DODATI?

    Kada se radi read_all_records(), funkcija treba da pronadje prvi wal zapis. To ne mora biti wal_001.log, zbog brisanja
    Treba napraviti brisanje (low water mark valjda). Njoj se prosledjuje index do kog se brisu segmenti (fajlovi)
    Neka podesavanja, recimo padding_character, sada je podesen na '0' radi debugovanja, treba da bude 0, cahche size, block size ...
*/

void debug_record2(Record r) {
    cout << "CRC: " << r.crc << endl;
    cout << "TIMESTAMP: " << r.timestamp << endl;
    cout << "TOMBSTONE: " << int(r.tombstone) << endl;
    cout << "KEYSIZE: " << r.key_size << endl;
    cout << "VALUESIZE: " << r.value_size << endl;
    cout << "KEY: " << r.key << endl;
    cout << "VALUE: " << r.value << endl;
}

void input_test_data(Wal& w1) {
    info("Added 40 records");
    /* w1.put("apple", "fruit");
    w1.put("dog", "animal");
    
    w1.put("rose", "flower");
    w1.put("ja sam mali paradajz i zivim u basti", "bas bi bilo lepo kad bi se prepustio masti");

    return;*/
    w1.put("oak", "tree");
    w1.put("sparrow", "bird");
    w1.put("shark", "fish");
    w1.put("iron", "metal");
    w1.put("mercury", "planet");
    w1.put("violin", "instrument");
    w1.put("blue", "color");
    w1.put("python", "language");
    w1.put("java", "coffee");
    w1.put("winter", "season");
    w1.put("happiness", "emotion");
    w1.put("oxygen", "element");
    w1.put("computer", "machine");
    w1.put("earth", "planet");
    w1.put("gold", "metal");
    w1.put("diamond", "gem");
    w1.put("river", "water");
    w1.put("mountain", "landform");
    w1.put("sun", "star");
    w1.put("moon", "satellite");
    w1.put("paper", "material");
    w1.put("pen", "tool");
    w1.put("book", "object");
    w1.put("courage", "virtue");
    w1.put("freedom", "concept");
    w1.put("justice", "ideal");
    w1.put("laptop", "device");
    w1.put("rain", "weather");
    w1.put("cloud", "sky");
    w1.put("time", "dimension");
    w1.put("speed", "quantity");
    w1.put("energy", "power");
    w1.put("peace", "state");
    w1.put("love", "feeling");
    w1.put("music", "art");
    w1.put("dream", "thought");
}

void input_test_data2(Wal& w1) {
    info("Added 10 records");
    w1.put("the sun rises in the east", "natural phenomenon");
    w1.put("a journey of a thousand miles begins with a single step", "philosophical saying");
    w1.put("the pen is mightier than the sword", "proverb");
    w1.put("to be or not to be, that is the question", "Shakespeare's quote");
    w1.put("life is what happens when you're busy making other plans", "John Lennon quote");
    w1.put("actions speak louder than words", "common saying");
    w1.put("a picture is worth a thousand words", "visual expression");
    w1.put("the early bird catches the worm", "motivational saying");
    w1.put("in the middle of difficulty lies opportunity", "Albert Einstein quote");
    w1.put("you miss 100% of the shots you don't take", "Wayne Gretzky quote");
}

void delete_more(Wal& w1) {
    info("Deleted 6 records");
    w1.del("apple");
    w1.del("cloud");
    w1.del("time");
	w1.del("speed");
	w1.del("energy");
    w1.del("mountain");
}

void delete_more2(Wal& w1) {
    info("Deleted 3 records");
    w1.del("actions speak louder than words");
    w1.del("the pen is mightier than the sword");
    w1.del("you miss 100% of the shots you don't take");
}

void print_files_in_folder(const string& folder_path) {
    cout << "Files in folder: " << folder_path << endl;
    try {
        for (const auto& entry : fs::directory_iterator(folder_path)) {
            if (fs::is_regular_file(entry.status())) {
                cout << entry.path().filename().string() << endl;
            }
        }
    }
    catch (const fs::filesystem_error& e) {
        cerr << "Error accessing folder: " << e.what() << endl;
    }
}

void delete_all_files(string folderPath) {
    try {
        // Check if the folder exists
        if (fs::exists(folderPath) && fs::is_directory(folderPath)) {
            for (const auto& entry : fs::directory_iterator(folderPath)) {
                if (fs::is_regular_file(entry)) { // Check if it's a file
                    fs::remove(entry); // Delete the file
                    //std::cout << "Deleted: " << entry.path() << std::endl;
                }
            }
            std::cout << "All files in the folder have been deleted." << std::endl << std::endl;
        }
        else {
            std::cout << "Folder does not exist or is not a directory." << std::endl;
        }
    }
    catch (const fs::filesystem_error& e) {
        std::cerr << "Filesystem error: " << e.what() << std::endl;
    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

//ovo je zbog maina, nema veze sa projekom!
string log_directory = "../data/wal_logs";

int main() {
    cout << "creating wal\n";
    Block_manager bm;

    Wal w1(bm);
    
    vector<Record> ret1 = w1.get_all_records();
    for (Record r : ret1) {
        debug_record2(r);
        cout << endl;
    }
    //w1.put("djuka", "puska");

    //ensure_wal_folder_exists();


    // Step 2: Create a Wal object and test its methods
    cout << "Step 2: Testing Wal class methods...\n";
   

    info("Inputting test data...");

    input_test_data(w1);

    input_test_data2(w1);

    info("Deleting some records using delete_more functions...");
    delete_more(w1);
    delete_more2(w1);

    w1.put("apple", "FRUIT");

    // Step 3: Display files in 'wal_logs' after Wal methods execution
    cout << "\nStep 3: Files in '" << WAL_FOLDER << "' after Wal methods execution:\n";
    print_files_in_folder(WAL_FOLDER);
    cout << endl;

    // Step 4: Retrieve and display all records from Wal
    vector<Record> ret = w1.get_all_records();
    cout << "Step 4: Total records in Wal: " << ret.size() << endl;
    for (const Record& r : ret) {
        cout << (char)r.tombstone << " " << r.key << " " << r.value << endl;
    }
    cout << endl;
    

    // Step 5: Test find_min_segment and delete_old_logs functionality
    cout << "Step 5: Testing find_min_segment and delete_old_logs...\n";
    //cout << "Minimum segment: " << w1.find_min_segment() << endl;

    cout << "\nDeleting logs older than 'wal_002.log'...\n";
    w1.delete_old_logs("wal_002.log");
    //cout << "Minimum segment after deletion: " << w1.find_min_segment() << endl;
    
    print_files_in_folder(log_directory);
    cout << endl;
    cout << "Deleting logs older than 'wal_004.log'...\n";
    w1.delete_old_logs("wal_004.log");
    //cout << "Minimum segment after deletion: " << w1.find_min_segment() << endl;

    // Step 6: Final display of files in 'wal_logs'
    cout << "\nFiles in '" << log_directory << "' after all operations:\n";
    print_files_in_folder(log_directory);


    return 0;
    
}
