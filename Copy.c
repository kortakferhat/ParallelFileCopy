/**
 * @author : Ferhat Kortak 
 * School Number : 2015510099
 * e-mail : ferhat.kortak@ceng.deu.edu.tr
 * 2018/2019 Fall Semester
 * Operating System 
 * Homework III
 * Dept. Of Computer Engineering
 * 
 * Sources
 * classroom.google.com
 * 
 * Compile & Run
 * gcc 2015510099.c -o move -lpthread -lrt
 * ./move /home/ferhat/Desktop/HW3/Practice/source.txt /home/ferhat/Desktop/HW3/Practice/destination.txt 5
*/

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <fcntl.h>
#include <stdlib.h>
#include <aio.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>

// Definitions
#define FILE_SIZE 255
int SUBBUFFERSIZE = 0;
int BUF_SIZE = 0;
int NUM_THREADS = 0;

// Global Variables
int file_length = 0;
int modulus = 0;
int total_workcount = 0;
int fd_source = -1, fd_destination = -1;
char *source;
char *destination;

// Mutual Exclusion
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

int source_file_generator()
{
    // Prepare the empty string
    int i = 0;
    fd_source = open(source, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

    if (fd_source == -1)
    {
        perror("open");
        printf("error");
        return -1;
    }
    char currentchar[1];
    printf("Working...\n");
    for (i = 0; i < FILE_SIZE; i++)
    {
        currentchar[0] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"[random() % 26];
        if (write(fd_source, currentchar, 1) != 1)
        {
            write(2, "There was an error writing to testfile.txt\n", 43);
            return -1;
        }
    }
    printf("File Creation Is Done!\n");
    close(fd_source);
    return 0;
}

// Multithreaded Read and Write
void *copy(void *param)
{
    // Initialize Variables
    int id = *(int *)param;
    int _buffersize = 0;
    int start_index = 0;
    int offset = 0;
    start_index = id * BUF_SIZE;

    // Control Blocks
    struct aiocb aio_source, aio_destination;

    int err = 0, ret = 0;

    // Reading section -------------------------------------------------------------------------
    char *read_data;
    _buffersize = BUF_SIZE;
    if ((id == NUM_THREADS-1) && (file_length % BUF_SIZE > 0))
    {
        _buffersize += modulus;
    }
        
    read_data = malloc(sizeof(char) * _buffersize);

    // Open the source file
    fd_source = open(source, O_RDONLY);
    fd_destination = open(destination, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

    // Error control
    if (fd_source == -1)
    {
        perror("open");
        pthread_exit(NULL);
    }

    memset(&aio_source, 0, sizeof(aio_source));

    aio_source.aio_fildes = fd_source;
    aio_source.aio_buf = read_data;
    aio_source.aio_nbytes = _buffersize;
    aio_source.aio_offset = start_index;
    aio_read(&aio_source);

    // Stop when no error
    while (aio_error(&aio_source) == EINPROGRESS){}
    err = aio_error(&aio_source);
    ret = aio_return(&aio_source);

    if (err != 0)
    {
        printf("Error at aio_error() : %s\n", strerror(err));
        close(fd_source);
        exit(2);
    }

    if (ret != _buffersize)
    {
        printf("Error at aio_return()\n");
        close(fd_source);
        exit(2);
    }
    printf("\nSuccessfully Readed!\n");

    // Writing Section -------------------------------------------------------------------------------

    // Calculate number of read per thread(Modulus will be added to first thread)
    int work_count = 0;
    work_count = _buffersize / SUBBUFFERSIZE;
    
    if ((modulus > 0 && (id == NUM_THREADS-1)) || BUF_SIZE%SUBBUFFERSIZE>0)
    {
        work_count++;
    }
    // All of the AIO system calls use a Control Block to keep track of the state of an operation
    // Read from file
    printf("Thread : %d\tBuffer Size: %d\tSub Buffer Size: %d\n",id,_buffersize,SUBBUFFERSIZE);
    printf("Thread : %d\tTotal Number of Work: %d\n",id,work_count);
    int i = 0;
    char current_data[SUBBUFFERSIZE];
    int dynamic_bufsize = 0;
    for (i = 0; i < work_count; i++)
    {
        total_workcount++;
        if(i==work_count-1 && BUF_SIZE%SUBBUFFERSIZE>0 && id!=(NUM_THREADS-1))
        {
            dynamic_bufsize = BUF_SIZE%SUBBUFFERSIZE;
            current_data[dynamic_bufsize] = '\0';
        }
        else if(modulus>0 && i==(work_count-1) && id == (NUM_THREADS-1))
        {
            dynamic_bufsize = modulus + (BUF_SIZE%SUBBUFFERSIZE);
            current_data[dynamic_bufsize] = '\0';
        }
        else
        {
            dynamic_bufsize = SUBBUFFERSIZE;
        }
        strncpy(current_data, read_data + offset, dynamic_bufsize);
        
        // Move to destination
        memset(&aio_destination, 0, sizeof(aio_destination));
        aio_destination.aio_fildes = fd_destination;
        aio_destination.aio_buf = current_data;
        aio_destination.aio_nbytes = dynamic_bufsize;
        aio_destination.aio_offset = start_index + offset;
        if (aio_write(&aio_destination) == -1)
        {
            printf(" Error at aio_write(): %s\n", strerror(errno));
            close(fd_destination);
            exit(2);
        }
        while (aio_error(&aio_destination) == EINPROGRESS)
        {
        }
        err = aio_error(&aio_destination);
        ret = aio_return(&aio_destination);

        if (err != 0)
        {
            printf("Error at aio_error() : %s\n", strerror(err));
            close(fd_destination);
            exit(-1);
        }

        if (ret != dynamic_bufsize)
        {
            printf("Error at aio_return()\n");
            close(fd_destination);
            exit(-1);
        }
        // User Display
        printf("Thread: %d -> %d%%\tCompleted: %d%%\n ", id, (i * 100) / work_count, (total_workcount * 100) / (work_count * NUM_THREADS));
        offset += SUBBUFFERSIZE;
    }
    printf("Writing is Done by %d\n\n", id);
}

// *argv[] is the parameter of the main method which will be called at the run operation
int main(int argc, char *argv[])
{
    // Variables
    int source_fd = 0, dest_fd = 0;
    NUM_THREADS = atoi(argv[3]);

    int thread_id[NUM_THREADS];
    // Threads
    pthread_t read_threads[NUM_THREADS];
    source = (char *)malloc(200);
    destination = (char *)malloc(200);

    strcpy(source, argv[1]);
    strcpy(destination, argv[2]);

    printf("Source File : %s\n", source);
    printf("Destination File : %s\n", destination);
    printf("Number of Threads : %d\n", NUM_THREADS);
    FILE* fp;
    fp = fopen(source,"w+");
    fclose(fp);

    fp = fopen(destination,"w+");
    fclose(fp);


    if (source_file_generator() == -1)
    {
        perror("source");
        return EXIT_FAILURE;
    }

    // Open the source file
    source_fd = open(source, O_RDONLY);
    dest_fd = open(destination, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    // Error control
    if (source_fd == -1)
    {
        perror("open");
        return EXIT_FAILURE;
    }

    // Calculating remaining part of the file w.r.t. BUF_SIZE
    file_length = lseek(source_fd, 0, SEEK_END);
    BUF_SIZE = file_length / NUM_THREADS;
    modulus = file_length % BUF_SIZE;
    printf("BUFFER SIZE : %d\n",BUF_SIZE);
    lseek(source_fd, 0, SEEK_SET);
    close(source_fd);

    printf("File Size : %d\n", file_length);
    SUBBUFFERSIZE = (BUF_SIZE > 1000) ? 1000 : BUF_SIZE;
    int i;
    for (i = 0; i < NUM_THREADS; i++)
    {
        thread_id[i] = i;
    }
    // Create the threads.
    for (i = 0; i < NUM_THREADS; i++)
    {
        pthread_create(read_threads + i, NULL, &copy, (void *)&thread_id[i]);
    }
    for (i = 0; i < NUM_THREADS; i++)
    {
        pthread_join(read_threads[i], NULL);
    }
    printf("\nCopying is Done!\n");
    close(fd_destination);
    close(fd_source);
}