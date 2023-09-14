#include <mpi.h>
#include <string>
#include <algorithm>
#include <vector>
#include <stdio.h>
#include <ctype.h>
#include <sstream>
#include <fstream>
#include <iostream> 
#include <map>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

using namespace std;

typedef struct {
	char fileName[100];
	char cuvant[100];
	char frecventa[100];
} element;

bool comparare(const element *a, const element *b) {
	if (strcmp(a->cuvant, b->cuvant) < 0)
		return true;
	else if (strcmp(a->cuvant, b->cuvant) > 0)
		return false;
	else
		return strcmp(a->fileName, b->fileName) < 0;
}


int mapping(int nr) {
	string inputDir = "sursa";
	string outputDir = "output";
	string fileName = to_string(nr) + ".txt";
	string outputFile = to_string(nr) + ".txt";

	FILE* fisin, *fisout;

	if (fopen_s(&fisin, (inputDir + "//" + fileName).c_str(), "r") != 0) {
		printf("Nu s-a putut dechide fisierul de input %d.txt", nr);
		return EXIT_FAILURE;
	}

	if (fopen_s(&fisout, (outputDir + "//" + outputFile).c_str(), "w") != 0) {
		printf("Nu s-a putut deschide fisierul de output %d.txt!", nr);
		return EXIT_FAILURE;
	}

	string word = "";
	char ch = 0;
	map<string, int> wordCount;

	while (!feof(fisin)) {
		ch = getc(fisin);
		ch = tolower(ch);
		if (isalpha(ch)) {
			word += ch;
		}
		else {
			if (!word.empty()) {
				++wordCount[word];
				word.clear();
			}
		}
	}

	for (const auto &[word, count] : wordCount) {
		string data = "<" + fileName + ",{" + word + ":" + to_string(count) + "}>\n";
		fputs(data.c_str(), fisout);
	}

	fclose(fisin);
	fclose(fisout);

	return EXIT_SUCCESS;
}


void sortare(const char *fileName, const char *fileSort) {
	FILE *input = fopen(fileName, "r");
	if (!input) {
		printf("Nu am putut deschide fisierul!");
		return;
	}
	FILE *output = fopen(fileSort, "w");
	if (!output) {
		printf("Nu am putut deschide fisierul!");
		return;
	}

	element elements[1000];
	int elementCount = 0;

	char line[1000];

	while (fgets(line, sizeof line, input) != NULL) {

		char *openBracket = strchr(line, '<');
		char *closeBracket = strchr(line, '>');
		if (openBracket == NULL || closeBracket == NULL)
			continue;

		*closeBracket = '\0';

		char *comma = strchr(openBracket, ',');
		if (comma == NULL)
			continue;

		*comma = '\0';

		strcpy(elements[elementCount].fileName, openBracket + 1);
		char *openBrace = strchr(comma, '{');
		if (openBrace == NULL)
			continue;

		char *closeBrace = strchr(comma, '}');
		if (closeBrace == NULL)
			continue;

		*closeBrace = '\0';

		char *colon = strchr(openBrace, ':');
		if (colon == NULL)
			continue;

		*colon = '\0';

		strcpy(elements[elementCount].cuvant, openBrace + 1);
		strcpy(elements[elementCount].frecventa, colon + 1);

		elementCount++;
	}

	qsort(elements, elementCount, sizeof(element),
		(int(*)(const void *, const void *))comparare);

	for (int i = 0; i < elementCount; i++)
		fprintf(output, "<%s,{%s:%s}>\n", elements[i].fileName,
			elements[i].cuvant, elements[i].frecventa);

	fclose(input);
	fclose(output);
}

int main(int argc, char* argv[])
{
	MPI_Init(&argc, &argv);
	int size;
	int rank;
	int muncitori;
	int primit;
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Status status;

	muncitori = size - 1;
	
	if (rank == 0) {

		for (int i = 1; i <= muncitori; ++i) {
			MPI_Send(&i, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
			
		}
	}
	else {

		MPI_Recv(&primit, 1, MPI_INT, 0, 0, MPI_COMM_WORLD,&status);
		mapping(primit);
	}

	if (rank == 0)
	{
		for (int i = 1; i <= muncitori; i++)
			MPI_Send(&i, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
	}
	else
	{
		MPI_Recv(&primit, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);

		
		string outSort = "outSort" + to_string(primit) + ".txt";
		string inputMapped = "outMapped" + to_string(primit) + ".txt";

		const char outSortChar[outSort.length() + 1];
		strcpy(outSortChar, outSort.c_str());

		const char inputMappedChar[inputMapped.length() + 1];
		strcpy(inputMappedChar, inputMapped.c_str());

		sortare(inputMappedChar, outSortChar);
		
	}

	MPI_Finalize();

	return EXIT_SUCCESS;
}
