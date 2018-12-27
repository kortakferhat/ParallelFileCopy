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
#define FILE_SIZE 100000
#define BUF_SIZE 10
int NUM_THREADS = 0;

// Global Variables
int file_length = 0; 
int modulus = 0;
int fd_source = -1, fd_destination = -1;
char* source;
char* destination;

// Mutual Exclusion
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

int source_file_generator()
{
    char data[FILE_SIZE];
    // Prepare the empty string
    int i = 0;
    for(;i < FILE_SIZE;i++)
    {
        // Random letter
        data[i] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ "[random () % 27];
    }
    //printf("%s\n",data);
    fd_source = open (source, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

    if (fd_source == -1) 
    {
        perror("open");
        printf("error");
        return -1;
    }
    if (write(fd_source, data, FILE_SIZE) != FILE_SIZE) {
        write(2, "There was an error writing to testfile.txt\n", 43);
        return -1;
    }
    close(fd_source);
    return 0;
}

// Initialize file then fill the file with full of zeros
int fillempty()
{
    char data[file_length];
    // Prepare the empty string
    int i = 0;
    for(;i < file_length;i++)
    {
        data[i] = '0';
    }

    fd_destination = open (destination, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

    if (fd_destination == -1) 
    {
        perror("open");
        return -1;
    }
    if (write(fd_destination, data, file_length) != file_length) {
        write(2, "There was an error writing to testfile.txt\n", 43);
        return -1;
    }
    close(fd_destination);
    return 0;
}

// Multithreaded Read and Write
void* read_write(void *param)
{
    // Initialize Variables
    int id = *(int*)param;
    printf("ID : %d",id);
    int start_index = 0;
    int offset = 0;
    start_index = id*BUF_SIZE;

    // Control Blocks
    struct aiocb aio_source,aio_destination; 
    offset = start_index;
    aio_source.aio_offset = start_index;

    int err=0,ret=0;
    char current_data[BUF_SIZE];

    // Calculate number of read per thread(Modulus will be added to first thread)
    int work_count = file_length/(NUM_THREADS*BUF_SIZE);
    if(modulus>0 && id == 0)
    {  
        work_count++;
    }

    // Open the source file
    fd_source = open(source, O_RDONLY);
    fd_destination = open(destination, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

     // Error control
    if(fd_source == -1)
    {
        perror("open");
        pthread_exit(NULL);
    }

    // AIO -> Async I/O
    // All of the AIO system calls use a Control Block to keep track of the state of an operation
    // Read from file
    int i = 0;
    printf("Thread Number : %d \t Work Count : %d \n",id,work_count);
    for(i = 0;i < work_count;i++)
    {
        memset(&aio_source, 0, sizeof(aio_source));

        aio_source.aio_fildes = fd_source;
        aio_source.aio_buf = current_data;
        aio_source.aio_nbytes = sizeof(current_data);
        aio_source.aio_offset = offset;
        aio_read(&aio_source);

        // Stop when no error
        while(aio_error(&aio_source) == EINPROGRESS){ }
        err = aio_error(&aio_source);
        ret = aio_return(&aio_source);
        
        if (err != 0) 
        {
            printf ("Error at aio_error() : %s\n", strerror (err));
            close (fd_source);
            exit(2);
        }

        if (ret != BUF_SIZE) 
        {
            printf("Error at aio_return()\n");
            close(fd_source);
            exit(2);
        }

        fprintf(stdout, "%s\tThread id : %d\tOffset : %d\n", current_data,id,offset);

        // Move to destination
        memset(&aio_destination, 0, sizeof(aio_destination));
        aio_destination.aio_fildes = fd_destination;
        aio_destination.aio_buf = current_data;
        aio_destination.aio_nbytes = sizeof(current_data);
        aio_destination.aio_offset = offset;
        if (aio_write(&aio_destination) == -1) {
            printf(" Error at aio_write(): %s\n", strerror(errno));
            close(fd_destination);
            exit(2);
        }
        while (aio_error(&aio_destination) == EINPROGRESS) { }
        err = aio_error(&aio_destination);
        ret = aio_return(&aio_destination);

        if (err != 0) {
            printf ("Error at aio_error() : %s\n", strerror (err));
            close (fd_destination);
            exit(-1);
        }

        if (ret != sizeof(current_data)) {
            printf("Error at aio_return()\n");
            close(fd_destination);
            exit(-1);
        }
        // Move the cursor to next iteration
        offset += NUM_THREADS*BUF_SIZE;
    }
}


// *argv[] is the parameter of the main method which will be called at the run operation
int main(int argc,char *argv[])
{
    // Variables
    int source_fd = 0, dest_fd = 0;
    NUM_THREADS = atoi(argv[3]);
    
    int thread_id[NUM_THREADS];
    // Threads
    pthread_t read_threads[NUM_THREADS];
    source = (char*)malloc(200);
    destination = (char*)malloc(200);

    strcpy(source, argv[1]);
    strcpy(destination, argv[2]);
    
    printf("Source File : %s\n",source);
    printf("Destination File : %s\n",destination);
    printf("# of Threads : %d\n",NUM_THREADS);
    if(source_file_generator() == -1)
    {
        perror("source");
        return EXIT_FAILURE;
    }
    
    
    // Open the source file
    source_fd = open(source, O_RDONLY);
    dest_fd = open(destination, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
     // Error control
    if(source_fd == -1)
    {
        perror("open");
        return EXIT_FAILURE;
    }

    // Calculating remaining part of the file w.r.t. BUF_SIZE
    file_length = lseek(source_fd,0,SEEK_END);
    modulus = file_length%BUF_SIZE;
    lseek(source_fd,0,SEEK_SET);
    close(source_fd);

    if(fillempty() == -1)
    {
        perror("empty");
        return EXIT_FAILURE;
    }

    printf("File Size : %d\n",file_length);
    printf("Job per Thread : %d\n",file_length/(NUM_THREADS*BUF_SIZE));

    int i;
    for(i = 0;i < NUM_THREADS;i++)
    {
        thread_id[i] = i;
        printf("i : %d, th[] %d \n",i,thread_id[i]);
    }
   	// Create the threads.
	for (i = 0; i < NUM_THREADS; i++)
	{
		pthread_create(read_threads + i, NULL, &read_write, (void *)&thread_id[i]);
	}
    for(i = 0;i < NUM_THREADS;i++)
    {
        pthread_join(read_threads[i], NULL);
    }
    printf ("Reading is finished\n");
    close(fd_destination);
    close(fd_source);
}

/**
 * Appendix
 * Source : https://www.gsp.com/cgi-bin/man.cgi?section=2&topic=open
O_RDONLY        open for reading only
O_WRONLY        open for writing only
O_RDWR          open for reading and writing
O_EXEC          open for execute only
O_NONBLOCK      do not block on open
O_APPEND        append on each write
O_CREAT         create file if it does not exist
O_TRUNC         truncate size to 0
O_EXCL          error if create and file exists
O_SHLOCK        atomically obtain a shared lock
O_EXLOCK        atomically obtain an exclusive lock
O_DIRECT        eliminate or reduce cache effects
O_FSYNC         synchronous writes
O_SYNC          synchronous writes

* AIO Struct
struct aiocb {
int aio_fildes; //file descriptor
    // The file descriptor on which the I/O operation is to be performed

off_t aio_offset; //file offset
    // This is the file offset at which the I/O operation is to be performed.

volatile void *aio_buf; //location of buffer
    // This is the buffer used to transfer data for a read or write operation.

size_t aio_nbytes; //length of transfer
    // This is the size of the buffer pointed to by aio_buf. 

    // Operation codes for 'aio_lio_opcode':
enum { LIO_READ, LIO_WRITE, LIO_NOP };

    aio_read — asynchronous read from file
int aio_read( struct aiocb *aiocbp // control block );
    // Returns 0 on success or -1 on error (sets errno) 

    aio_write — asynchronous write to file
int aio_write(struct aiocb *aiocbp // control block);
    // Returns 0 on success or -1 on error (sets errno)

    aio_return — retrieve return status of asynchronous I/O operation
ssize_t aio_return(struct aiocb *aiocbp // control block );
    // Returns operation return value or -1 on error (sets errno) 

    aio_error — retrieve error status for asynchronous I/O operation
int aio_error(const struct aiocb *aiocbp // control block );
    // Returns 0, errno value, or EINPROGRESS (does not set errno) 

    aio_cancel — cancel asynchronous I/O request
int aio_cancel( int fd, // file descriptor struct aiocb *aiocbp // control block );
    // Returns result code or -1 on error (sets errno)

*/