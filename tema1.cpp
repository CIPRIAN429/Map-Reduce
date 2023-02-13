#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <math.h>
#include <string.h>
#include <stdbool.h>
#include <bits/stdc++.h>
using namespace std;

double file_size(FILE *file){
	fseek(file, 0L, SEEK_END);
	return ftell(file);
}

pthread_barrier_t barrier;

typedef struct thread_info {
    int index;
    int M;
    int R;
	char files[3000][30]; // pentru map
	int no_files; // pentru map
	map <int, vector<long long> > m; // listele partiale
	set <long long> reduce; // pentru reduce
	double current_size;
} thread_info;



double min (double a, double b) {
	if(a > b)
		return b;
	return a;
}

bool perfect (long long number, int exponent) {
	
	long long lo = 2;
	long long hi = number;
	long long mid;
	long long val;
	// cautare binara  interval [1,n] pentru
	// detectare numar putere perfecta
	while(hi - lo >= 1){
		mid = (hi + lo) / 2;
		int aux = exponent;
		val = 1;
		while(aux > 0){
			val *= mid;
			if(val > number){
				break;
			}
			aux--;
		}
		if(val > number)
			hi = mid;
		else if(val < number)
			lo = mid +1;
		else break;
	}

	if(val == number)
		return true;
	return false;
	
}

void *thread_function(void *arg)
{
	thread_info thread = *((thread_info *)(arg));
	static vector <map <int, vector<long long> > > common;
	// partea de map	
    if (thread.index < thread.M) {
		for (int i = 0; i < thread.no_files; i++) {
			char line[30];
			FILE *file = fopen(thread.files[i], "r");
			fgets (line, 30, file);
			while (fgets (line, 30, file) != NULL) {
				for (int j = 2; j <= thread.R + 1; j++) { // j este supraunitar
					long long number = atoi(line);
					if ((number != 0 && perfect (number, j) == true)  || number == 1)
						thread.m[j-2].push_back(number); 
				}
			}
			
			fclose(file);
		}
		common.push_back(thread.m);
		
	}
   
   	pthread_barrier_wait(&barrier);
	// partea de reduce 
    if (thread.index >= thread.M) {
		size_t size = common.size();
		for(size_t i = 0; i < size; i++){
			size_t nr = common[i][thread.index - thread.M].size();
			for(size_t j = 0; j < nr; j++)
				thread.reduce.insert(common[i][thread.index - thread.M][j]);

		}
		char s[30];
		int result = thread.index - thread.M + 2;
		sprintf(s, "out%d.txt", result);
		FILE *g = fopen(s, "w");
		int size2 = thread.reduce.size();
		fprintf(g, "%d", size2);
		fclose(g);
    }

	pthread_exit(NULL);
}



int main(int argc, char *argv[])
{
    int M, R, MR, i, r;
	
    if(argc < 4) {
 		printf("Numar insuficient de parametri: ./tema1 M R file\n");
 		exit(1);
 	}
	
    M = atoi(argv[1]);
    R = atoi(argv[2]);
	pthread_barrier_init(&barrier, NULL, M+R);
    MR = M + R;
    thread_info threads[MR]; // informatii despre thread-uri
    pthread_t tid[MR];
	FILE  *file2;
	char line[30];
	
	file2 = fopen(argv[3], "r");
	fgets (line, 30, file2);

	if (file2 == NULL) {
        printf("Error: file pointer is null.");
        exit(1);
    }

	double min1 = 0;
	
	for(int j = 0; j < M; j++) {
		threads[j].current_size = 0;
		threads[j].no_files = 0;		
	}
	
	fgets (line, 30, file2);
	//impartire echilibrata fisiere	
	bool aux2 = true;
	while(aux2){
		for(int j = 0; j < M; j++) {
			if(threads[j].current_size == min1){
				int size = strlen(line);
				if (line[size - 1] == '\n')
					line[size - 1] = '\0';
				FILE *in_file = fopen(line, "r");
				strcpy(threads[j].files[threads[j].no_files], line);
	 			threads[j].no_files++;
				threads[j].current_size += file_size(in_file);
				fclose(in_file);
				if(fgets (line, 30, file2) == NULL){
					aux2 = false;
					break;
				}
			}
		}
		min1 = threads[0].current_size;
		for(int j = 1; j < M; j++) {
			if(threads[j].current_size < min1){
				min1 = threads[j].current_size;
			}
		}
	}
	fclose(file2);
	//pornire thread-uri	
    for (i = 0; i < MR; i++) {
		threads[i].index = i;
        threads[i].M = M;
        threads[i].R = R;
		r = pthread_create(&tid[i], NULL, thread_function, &threads[i]);
        if (r) {
	  		printf("Eroare la crearea thread-ului %d\n", i);
	  		exit(-1);
		}
	}

    for (i = 0; i < MR; i++) {
		pthread_join(tid[i], NULL);
	}
    
	pthread_barrier_destroy(&barrier);

	return 0;
}
