#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>

#include <pthread.h>

#include "strmap.h"

/**
 * FileParse
 *
 * Author: John Qiao
 * 
 * Email: qiaoshun8888@gmail.com  Date: Feb. 24, 2014  10:59:11 PM
 *
 * Homework descrption:
 * 
 * Part 1: 
 * This program is currently a single threaded process. 
 * Your objective is to make this a multi-threaded program such that the reading of files is done concurrently.
 * To do this, you’ll need to use the pthreads library as discussed in class. 
 * Be careful to ensure that all threads have completed running before exiting and that you have destroyed any objects you created.
 *
 * Part 2:
 * The current fileparse.c loops through logs and displays the line and the size of the line. 
 * The objective of the second part of this exercise is to get a count of the unique number of IP addresses in the log file. 
 * Note: the IP address is the first set of numbers on each line and it indicates the IP of the person that visited that page.
 * 
 * You can take any approach (in C – no calls to outside programs) you would like to do this 
 * but keep in mind that it must be free of race conditions. 
 * I would recommend using synchronization primitives provided by the pthreads library 
 * but other techniques such as using thread local storage would also be acceptable.
 *
 * Implementation: Using Map datastructure to record the ip address we have found in all the threads.
 * If the ip address already in the map, we ignore it, otherwise we add the ip into the map.
 * Everytime we add an ip into the map, we increase the global variable ip_count by 1. 
 * Finally, we output ip_count as the result. During the whole procedure, our global ip_count and map
 * are well protected by mutex variable map_lock, so that only one thread can update and access the shared data.
 * 
 * Analytics: 
 * Time complexity:	  O(n) 
 * Space complexity:  O(n)
 * (n is the number of the ip address in all of the files)
 *
 * How to run this program:
 *
 * For OSX:
 * 1. Under hw1 directory, enter: gcc fileparse.c strmap.c -o fileparse
 * 2. Then enter: ./fileparse access_logs/ 5
 * 
 * For Ubuntu:
 * 1. Under h1 directory, enter: gcc fileparse.c strmap.c -lpthread -ofileparse
 * 2. Then enter: chmod +x ./fileparse
 * 3. Then enter: ./fileparse access_logs/ 5
 *
 * Reference links:
 * 
 * 1. https://computing.llnl.gov/tutorials/pthreads/
 * 2. http://siber.cankaya.edu.tr/ozdogan/GraduateParallelComputing.old/ceng505/node87.html
 * 3. http://pokristensson.com/strmap.html
 *
 */

int num_of_files;
int num_of_threads;
char *dir_name;

pthread_mutex_t map_lock; 
int ip_count;
StrMap *map;

void *loadFiles(void *thread_id); 
int countFiles(DIR *dir);

int main(int argc, char *argv[]) {
	static const char *ERROR_ARGUMENTS = "Expected arguments <directory> <number of threads>";
	static const int MAP_SIZE = 200; // define the size of the map

	DIR *dir;	// directory stream
	FILE *file;	// file stream
	
	char *line = NULL;	// pointer to 
	size_t len = 1000;	//the length of bytes getline will allocate
	size_t read;

	char full_filename[256];	// will hold the entire file name to read		

	// check the arguments
	if(argc < 3) {
		printf("Not enough arguments supplied. \n%s \n", ERROR_ARGUMENTS);
		return -1;
	}
	if(argc > 3) {
		printf("Too many arguments supplied. \n%s \n", ERROR_ARGUMENTS);
		return -1;
	}

	// Check the directory
	dir_name = argv[1];
	if ((dir = opendir(dir_name)) == NULL) {
		printf("Error: cannot open directory (%s) \n", argv[1]);
		return -1;
	}

	// Check the argument of the number of threads
	if ((num_of_threads = atoi(argv[2])) <= 0) {
		printf("Error: Invalid arguemnts. The number of threads should > 0.");
		return -1;
	}

	printf("========================== Running Logs Begin ==========================\n");
	printf("Arguments:\n");
	printf("- directory: %s\n", dir_name);
	printf("- number of threads: %d\n", num_of_threads);

	printf("Initialization:\n");
	printf("Create Map Data Structure, size = %d\n", MAP_SIZE);
	map = sm_new(MAP_SIZE);
	if (map == NULL) {
		printf("Error: Create map failed.");
		return -1;
	}
	/* Initialize mutex variable objects */
  	pthread_mutex_init(&map_lock, NULL);

  	/* Count the number of the files under the directory */
	printf("\nOutput:\n");
	num_of_files = countFiles(dir);
	closedir (dir); // Close the directory structure
	printf("Number of files: %d\n", num_of_files);

	pthread_t threads[num_of_threads];
	pthread_attr_t attr;

	/* Get the default attributes */
	pthread_attr_init(&attr);
	int result;

	/* Create threads */
	long i;
	for (i = 0; i < num_of_threads; i++) {
		printf("Create thread %ld\n", i);
		result = pthread_create(&threads[i], &attr, loadFiles, (void *)i);
		if (result) {
			printf("Error: return error code from pthread_create() [%d]\n", result);
			exit(-1);
		}
	}

	/* Wait for all threads to complete */
	for (i = 0; i < num_of_threads; i++) {
		pthread_join(threads[i], NULL);
	}

	printf("\nResult:\n");
	printf("Ip count: %d\n", ip_count);

	printf("========================== Running Logs End ==========================\n");

	/* Clean up and exit */
	pthread_attr_destroy(&attr);
	pthread_mutex_destroy(&map_lock);
	pthread_exit(NULL);

	return 0;
}



void *loadFiles(void *thread_id) {
	int num = num_of_files / num_of_threads; // how many files need to process
	int tid = (long)thread_id;
	int partition = tid * num; // calculate the partitions
	printf("loadFiles called, thread #%d, partition #%d\n", tid, partition);

	DIR *dir;	//directory stream
	FILE *file;	//file stream
	struct dirent *ent;			// directory entry structure
	char full_filename[256];	//will hold the entire file name to read
	char buf[255]; // buffer of the key of the map

	if ((dir = opendir (dir_name)) != NULL)  {
		int i = partition + 1;
		while (1) {
			int last_one = tid == num_of_threads - 1;
			if (last_one &&  i > num_of_files) { // If it's the last thread, it'll be terminated after loaded all the file left.
				break;
			}
			else if (!last_one && i > partition + num) {
				break;
			}
			// open the file
			snprintf(full_filename, sizeof full_filename, "./%saccess%d.log\0", dir_name, i);
			file = fopen(full_filename, "r");
			if (file == NULL) {
				printf("Error: Not found file %s\n", full_filename);
			}
			else {
				long lines = 0;
				char *line = NULL;	// pointer to 
				size_t len = 1000;	//the length of bytes getline will allocate
				size_t read;
				char *ip = NULL;
				
				while ((read = getline(&line, &len, file)) != -1) {
					lines++;
					ip = strtok (line, " ");
					pthread_mutex_lock(&map_lock);  // lock the mutex associated with minimum_value and  update the variable as required
					int flag = sm_exists(map, ip); // sm_get
					if (flag == 0) { // Not found, add ip into map
						sm_put(map, ip, "");
						ip_count++; // Increase the global ip count
					}
					pthread_mutex_unlock(&map_lock); // unlock the mutex
				}
				printf("Openned file[tid=%d, i=%d]: \t%s \t(%ld lines)\n", tid, i, full_filename, lines);
				fclose(file);
			}
			i++;
		}
	}

	closedir(dir);

	pthread_exit(NULL); // terminate the thread
}

int countFiles(DIR *dir) {
	int count = 0;
	struct dirent *ent;	// directory entry structure
	while ((ent = readdir (dir)) != NULL)  {
		if(ent->d_type == DT_REG) {
			count++;
		}
	}
	return count;
}
