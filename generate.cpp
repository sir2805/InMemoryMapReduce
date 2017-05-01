#include <fstream>
#include <string>

using namespace std;

int main(int argc, char** argv) {
	int n;
	string filepath;
	int a, b;
	if (argc == 1) {
		n = 10;
		filepath = "out.txt";
		a = 0;
		b = 5;
	}
	else if (argc == 3) {
		string num = argv[1];
		n = stoi(num);
		filepath = argv[2];
		a = 0;
		b = n / 2;
	}
	else if (argc == 5) {
		string num = argv[1];
		n = stoi(num);
		filepath = argv[2];
		num = argv[3];
		a = stoi(num);
		num = argv[4];
		b = stoi(num);
	}
	else return 1;
	ofstream fout(filepath);
	for (int i = 0; i < n; i++) {
		fout << rand() % (b - a) + a;
		fout << '\t';
		fout << rand() % (b - a) + a;
		fout << '\n';
	}
	fout.close();
	return 0;
}