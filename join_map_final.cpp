#include <string>
#include <iostream>
//#include <fstream>

int main(int argc, char** argv) {
	std::string str;
	//std::ifstream cin("in.txt");
	std::getline(std::cin, str);
	std::size_t found = str.find_first_of('\t');
	std::string b = str.substr(0, found);
	str.erase(0, found + 1);
	while (found != std::string::npos) {
		found = str.find_first_of('\t');
		std::string a = str.substr(0, found);
		str.erase(0, found + 1);
		found = str.find_first_of('\t');
		std::string c = str.substr(0, found);
		str.erase(0, found + 1);
		std::cout << a;
		std::cout << '\t';
		std::cout << b;
		std::cout << '\t';
		std::cout << c;
		std::cout << '\n';
	}
	return 0;
}