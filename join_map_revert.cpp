#include <iostream>
#include <string>
using namespace std;

int main() {
	string kv;
	getline(cin, kv);
	int pos = kv.find_first_of('\t');
	string res = kv.substr(pos + 1);
	res += "\t1\t";
	res += kv.substr(0, pos);
	cout << res;
	return 0;
}
