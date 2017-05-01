#include <string>
#include <iostream>

int main() {
	std::string res;
	std::string b;
	std::string a;
	std::string c;
	for (std::string line; getline(std::cin, line); ) {
		size_t found = line.find_first_of('\t');
		if (b.empty()) {
			b = line.substr(0, found);
		}
		size_t found2 = line.find_first_of('\t', found + 1);
		if (found2 != std::string::npos) {
			a += '\t';
			a += line.substr(found2 + 1);
		}
		else {
			c += '\t';
			c += line.substr(found + 1);
		}
	}
	res += b;
	size_t founda = a.find_first_of('\t');
	while (founda != std::string::npos) {
		size_t cura = founda;
		founda = a.find_first_of('\t', founda + 1);
		size_t foundc = c.find_first_of('\t');
		while (foundc != std::string::npos) {
			size_t curc = foundc;
			foundc = c.find_first_of('\t', foundc + 1);
			res += a.substr(cura, founda);
			res += c.substr(curc, foundc);
		}
	}
	std::cout << res;
	return 0;
}