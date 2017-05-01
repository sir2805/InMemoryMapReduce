#include <string>
#include <iostream>
#include <sstream>
#include <regex>

int main(int argc, char** argv) {
	std::regex word("[^[:alpha:]|' ']+");
	const std::string format = "";
	for (std::string str; std::getline(std::cin, str); ) {
		str = std::regex_replace(str, word, format, std::regex_constants::format_default);
		std::transform(str.begin(), str.end(), str.begin(), ::tolower);
		std::stringstream stream;
		stream << str;
		for (std::string tmp; stream >> tmp; ) {
			std::cout << tmp << '\t' << 1 << std::endl;
		}
	}
}