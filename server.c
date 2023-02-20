
#include <signal.h>
#include <sys/stat.h>
#include "utils.h"
#include "kissdb.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>

#define MY_PORT                 06767
#define BUF_SIZE                1160
#define KEY_SIZE                 128
#define HASH_SIZE               1024
#define VALUE_SIZE              1024
#define MAX_PENDING_CONNECTIONS   10
#define QUEUE_SIZE                20

// Definition of the operation type.
typedef enum operation {
  PUT,
  GET
} Operation; 

// Definition of the request.
typedef struct request {
  Operation operation;
  char key[KEY_SIZE];  
  char value[VALUE_SIZE];
} Request;

// Definition of the database.
KISSDB *db = NULL;

/**
 * @name parse_request - Parses a received message and generates a new request.
 * @param buffer: A pointer to the received message.
 *
 * @return Initialized request on Success. NULL on Error.
 */
Request *parse_request(char *buffer) {
  char *token = NULL;
  Request *req = NULL;
  
  // Check arguments.
  if (!buffer)
    return NULL;
  
  // Prepare the request.
  req = (Request *) malloc(sizeof(Request));
  memset(req->key, 0, KEY_SIZE);
  memset(req->value, 0, VALUE_SIZE);

  // Extract the operation type.
  token = strtok(buffer, ":");    
  if (!strcmp(token, "PUT")) {
    req->operation = PUT;
  } else if (!strcmp(token, "GET")) {
    req->operation = GET;
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the key.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->key, token, KEY_SIZE);
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the value.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->value, token, VALUE_SIZE);
  } else if (req->operation == PUT) {
    free(req);
    return NULL;
  }
  return req;
}

/*
 * @name process_request - Process a client request.
 * @param socket_fd: The accept descriptor.
 *
 * @return
 */
int reader_count=0;
int writer_count=0;
pthread_mutex_t rw_mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t readers=PTHREAD_COND_INITIALIZER;
pthread_cond_t writers=PTHREAD_COND_INITIALIZER;
struct read_write{
	char response_str[BUF_SIZE];
	Request *request;
};
void reading(struct read_write hilfe){
	pthread_mutex_lock(&rw_mutex);
	do{
		pthread_cond_wait(&readers,&rw_mutex);
	}while(writer_count==1);
	reader_count++;
	pthread_mutex_unlock(&rw_mutex);
            // Read the given key from the database.
    if (KISSDB_get(db, hilfe.request->key, hilfe.request->value)){
        sprintf(hilfe.response_str, "GET ERROR\n");
    }else{
        sprintf(hilfe.response_str, "GET OK: %s\n", hilfe.request->value);
	}
	pthread_mutex_lock(&rw_mutex);
	reader_count--;
	if(reader_count==0){
		pthread_cond_broadcast(&writers);
	}
	pthread_mutex_unlock(&rw_mutex);
}
void writing(struct read_write hilfe){
	pthread_mutex_lock(&rw_mutex);
	do{
		if(writer_count==1){
			pthread_cond_wait(&readers,&rw_mutex);
		}else{
			pthread_cond_wait(&writers,&rw_mutex);
		}
	}while(writer_count==1 || reader_count>0);
	writer_count=1;
	pthread_mutex_unlock(&rw_mutex);
            // Write the given key/value pair to the database.
    if (KISSDB_put(db, hilfe.request->key, hilfe.request->value)){
        sprintf(hilfe.response_str, "PUT ERROR\n");
    }else{
        sprintf(hilfe.response_str, "PUT OK\n");
	}
	pthread_mutex_lock(&rw_mutex);
	writer_count=0;
	pthread_cond_broadcast(&readers);
	pthread_mutex_unlock(&rw_mutex);
}
void process_request(const int socket_fd) {
  char  request_str[BUF_SIZE];
    int numbytes = 0;
	struct read_write voithitiko;
    voithitiko.request = NULL;

    // Clean buffers.
    memset(voithitiko.response_str, 0, BUF_SIZE);
    memset(request_str, 0, BUF_SIZE);
    
    // receive message.
    numbytes = read_str_from_socket(socket_fd, request_str, BUF_SIZE);
    
    // parse the request.
    if (numbytes) {
      voithitiko.request = parse_request(request_str);
      if (voithitiko.request) {
        switch (voithitiko.request->operation) {
          case GET:
			reading(voithitiko);
            break;
          case PUT:
			writing(voithitiko);
            break;
          default:
            // Unsupported operation.
            sprintf(voithitiko.response_str, "UNKOWN OPERATION\n");
        }
        // Reply to the client.
        write_str_to_socket(socket_fd, voithitiko.response_str, strlen(voithitiko.response_str));
        if (voithitiko.request)
          free(voithitiko.request);
        voithitiko.request = NULL;
        return;
      }
    }
    // Send an Error reply to the client.
    sprintf(voithitiko.response_str, "FORMAT ERROR\n");
    write_str_to_socket(socket_fd, voithitiko.response_str, strlen(voithitiko.response_str));
}

/*
 * @name main - The main routine.
 *
 * @return 0 on success, 1 on error.
 */
typedef struct Ouras{
	int new_fd; 
	struct timespec start_time;
}Ouras;
Ouras *oura;
int head=0;
int tail=0;
int size=0;
void isagogi(struct Ouras *oura,struct Ouras *stixio){
  if((head==tail+1) || (head==0 && tail == QUEUE_SIZE-1)){
	  printf("full");
  }else if(head ==-1){
	  head=0;
	  tail=(tail+1)%QUEUE_SIZE; 
	  oura[tail].new_fd=stixio->new_fd;
	  oura[tail].start_time=stixio->start_time;
	  size++;
  }
}
void exagogi(struct Ouras *oura,struct Ouras *stixio){
  if(head==-1){
	 printf("empty");
  }else{
	     stixio->new_fd=oura[head].new_fd;
	     stixio->start_time=oura[head].start_time;
		 if(head==tail){
			 head=-1;
			 tail=-1;
		 }else{
			 head=(head+1)%QUEUE_SIZE;
		 }
		 size--;		 
  }	
} 
pthread_mutex_t queue_mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t full_queue=PTHREAD_COND_INITIALIZER;
pthread_cond_t empty_queue=PTHREAD_COND_INITIALIZER;
int completed_requests=0;
double total_service_time=0;
double total_waiting_time=0;
int c=0;
void *help(){
	struct Ouras new_stixio;
	struct timespec start,stop;
	do{
		if(c==1){
			close(new_stixio.new_fd);
			pthread_exit(0);
		}	
		pthread_mutex_lock(&queue_mutex);
		do{ 
			pthread_cond_wait(&full_queue,&queue_mutex);
		}while(head==-1);
		if(size<QUEUE_SIZE){
			pthread_cond_signal(&empty_queue);
		}
		exagogi(oura, &new_stixio);
		pthread_mutex_unlock(&queue_mutex);
		if(clock_gettime(CLOCK_MONOTONIC,&start)==0){
		    process_request(new_stixio.new_fd);
		    close(new_stixio.new_fd);       
		    if(clock_gettime(CLOCK_MONOTONIC,&stop)==0){
		        total_service_time=total_service_time+(stop.tv_sec-start.tv_sec)+(stop.tv_nsec-start.tv_nsec)/1000000000;
		        total_waiting_time=total_waiting_time+(start.tv_sec-new_stixio.start_time.tv_sec)+(start.tv_nsec-new_stixio.start_time.tv_nsec)/1000000000;
		        completed_requests=completed_requests+1;
		        pthread_exit(0);
			}else{
				ERROR("clock");
			}
		}else{
			ERROR("clock");
		}
	}while(1);
}
pthread_t *threads;
void signal_help(int x){
	c=1;
	/*for(int i=0; i<MAX_PENDING_CONNECTIONS; i++){
	    pthread_join(threads[i],NULL);
	}*/
	double y=total_service_time/completed_requests;
	double z=total_waiting_time/completed_requests;
	printf("\n");
	printf("Average Service Time:%f\n",y);
	printf("Average Waiting Time:%f\n",z);
	printf("Completed requests:%5d\n",completed_requests); //ksana
	exit(0);
}
int main() {

  int socket_fd,              // listen on this socket for new connections
      new_fd;                 // use this socket to service a new connection
  socklen_t clen;
  struct sockaddr_in  server_addr, // my address information
                       client_addr;// connector's address information
  struct timespec start;
  struct Ouras neo_stixio;
  
  // create socket
  if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    ERROR("socket()");

  // Ignore the SIGPIPE signal in order to not crash when a
  // client closes the connection unexpectedly.
  signal(SIGPIPE, SIG_IGN);
  
  // create socket adress of server (type, IP-adress and port number)
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);    // any local interface
  server_addr.sin_port = htons(MY_PORT);
  
  // bind socket to address
  if (bind(socket_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) == -1)
    ERROR("bind()");
  
  // start listening to socket for incomming connections
  listen(socket_fd, MAX_PENDING_CONNECTIONS);
  fprintf(stderr, "(Info) main: Listening for new connections on port %d ...\n", MY_PORT);
  clen = sizeof(client_addr);

  // Allocate memory for the database.
  if (!(db = (KISSDB *)malloc(sizeof(KISSDB)))) {
    fprintf(stderr, "(Error) main: Cannot allocate memory for the database.\n");
    return 1;
  }
  
  // Open the database.
  if (KISSDB_open(db, "mydb.db", KISSDB_OPEN_MODE_RWCREAT, HASH_SIZE, KEY_SIZE, VALUE_SIZE)) {
    fprintf(stderr, "(Error) main: Cannot open the database.\n");
    return 1;
  }
  
  oura=(Ouras*)malloc(QUEUE_SIZE*sizeof(Ouras)); //dimiourgia ouras
  
  threads=(pthread_t*)malloc(MAX_PENDING_CONNECTIONS*sizeof(pthread_t));
  for(int i=0; i<MAX_PENDING_CONNECTIONS; i++){
	  pthread_create(&threads[i],NULL,help,NULL);
  }
  /*sigset_t set;
  sigemptyset(&set);
  sigaddset(&set,SIGTSTP);
  pthread_sigmask(SIG_BLOCK,&set,NULL);*/
  signal(SIGTSTP,signal_help); 
	
  // main loop: wait for new connection/requests
  while (1) { 
    // wait for incomming connection
    if ((new_fd = accept(socket_fd, (struct sockaddr *)&client_addr, &clen)) == -1) {
        ERROR("accept()");
	}
	//fprintf(stderr, "(Info) main: Got connection from '%s'\n", inet_ntoa(client_addr.sin_addr)); 
    // got connection, serve request 
	neo_stixio.new_fd = new_fd;
	neo_stixio.start_time = start;
	pthread_mutex_lock(&queue_mutex);
	if(size ==QUEUE_SIZE) {   
		pthread_cond_wait(&empty_queue,&queue_mutex);
	}else{
		isagogi(oura,&neo_stixio);		
		pthread_cond_signal(&full_queue);   
	}
	pthread_mutex_unlock(&queue_mutex);
  }

  // Destroy the database.
  // Close the database.
  KISSDB_close(db);

  // Free memory.
  if (db)
    free(db);
  db = NULL;

  return 0; 
}

