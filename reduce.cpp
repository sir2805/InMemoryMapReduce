#include <string>
#include <iostream>
#include <map>

int main() {
	std::string word;
	std::cin >> word;
	int wordCount;
	std::cin >> wordCount;
	wordCount = 0;
	for (std::string curWord; std::getline(std::cin, curWord); ) {
		wordCount++;
	}
	std::cout << word << '\t' << wordCount << std::endl;
	return 0;
	/*std::map<std::string, int> words;
	for (std::string word; std::cin >> word;) {
		int curCount;
		std::cin >> curCount;
		
		if (words.find(word) == words.end()) {
			words.emplace(word, curCount);
		}
		else {
			words.at(word) += curCount;
		}
	}
	for (std::pair<std::string, int> word : words) {
		std::cout << word.first << '\t' << word.second << std::endl;
	}*/
}