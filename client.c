
#include "utils.h"
#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>

#define SERVER_PORT     06767
#define BUF_SIZE        2048 
#define MAXHOSTNAMELEN  1024
#define MAX_STATION_ID   128 
#define ITER_COUNT          1
#define GET_MODE           1
#define PUT_MODE           2
#define USER_MODE          3

/**
 * @name print_usage - Prints usage information.
 * @return
 */
void print_usage() {
  fprintf(stderr, "Usage: client [OPTION]...\n\n");
  fprintf(stderr, "Available Options:\n");
  fprintf(stderr, "-h:             Print this help message.\n");
  fprintf(stderr, "-a <address>:   Specify the server address or hostname.\n");
  fprintf(stderr, "-o <operation>: Send a single operation to the server.\n");
  fprintf(stderr, "                <operation>:\n");
  fprintf(stderr, "                PUT:key:value\n");
  fprintf(stderr, "                GET:key\n");
  fprintf(stderr, "-i <count>:     Specify the number of iterations.\n");
  fprintf(stderr, "-g:             Repeatedly send GET operations.\n");
  fprintf(stderr, "-p:             Repeatedly send PUT operations.\n");
}

/**
 * @name talk - Sends a message to the server and prints the response.
 * @server_addr: The server address.
 * @buffer: A buffer that contains a message for the server.
 *
 * @return
 */
void talk(const struct sockaddr_in server_addr, char *buffer) {
  char rcv_buffer[BUF_SIZE];
  int socket_fd, numbytes;
      
  // create socket
  if ((socket_fd = socket(PF_INET, SOCK_STREAM, 0)) == -1) {
    ERROR("socket()");
  }

  // connect to the server.
  if (connect(socket_fd, (struct sockaddr*) &server_addr, sizeof(server_addr)) == -1) {
    ERROR("connect()");
  }
  
  // send message.
  write_str_to_socket(socket_fd, buffer, strlen(buffer));
      
  // receive results.
  printf("Result: ");
  do {
    memset(rcv_buffer, 0, BUF_SIZE);
    numbytes = read_str_from_socket(socket_fd, rcv_buffer, BUF_SIZE);
    if (numbytes != 0)
      printf("%s", rcv_buffer); // print to stdout
  } while (numbytes > 0);
  printf("\n");
      
  // close the connection to the server.
  close(socket_fd);
}

/**
 * @name main - The main routine.
 */
struct help{ //gia na ta vlepoun oloi
	int mode;
	struct sockaddr_in server_addr;
    char snd_buffer[BUF_SIZE];
	int thread_number;
};
	
void *client_help(void *arg){
	struct help *voithia=arg;
	char *ok="ok";
	memset(voithia->snd_buffer, 0, BUF_SIZE); 
	if (voithia->mode == GET_MODE) {
	  // Repeatedly GET.
	  sprintf(voithia->snd_buffer, "GET:thread.%d",voithia->thread_number);
	} else if (voithia->mode == PUT_MODE) {
	  // Repeatedly PUT a random value.
	  sprintf(voithia->snd_buffer, "PUT:thread.%d:%d", voithia->thread_number,rand() % 65 + (-20) );
	}
	printf("Operation: %s\n", voithia->snd_buffer);
	talk(voithia->server_addr, voithia->snd_buffer);
	pthread_exit(0);
	return ok;
}

int main(int argc, char **argv) {
  char *host = NULL;  
  char *request = NULL;
  int option = 0;
  int count = ITER_COUNT;   
  struct hostent *host_info;
  struct help voithia;
  voithia.mode = 0;
   
  // Parse user parameters.
  while ((option = getopt(argc, argv,"i:hgpo:a:")) != -1) { 
    switch (option) {
      case 'h':
        print_usage();
        exit(0); 
      case 'a':
        host = optarg; 
        break;
      case 'i':
        count = atoi(optarg);
	break;
      case 'g':
        if (voithia.mode != 0) { 
          fprintf(stderr, "132 You can only specify one of the following: -g, -p, -o\n");
          exit(EXIT_FAILURE);
        }
        voithia.mode = GET_MODE;
        break;
      case 'p': 
        if (voithia.mode != 0) {
          fprintf(stderr, "139 You can only specify one of the following: -g, -p, -o\n");
          exit(EXIT_FAILURE);
        }
        voithia.mode = PUT_MODE;
        break;
      case 'o':
        if (voithia.mode != 0) {
          fprintf(stderr, "146 You can only specify one of the following: -r, -w, -o\n");
          exit(EXIT_FAILURE);
        }
        voithia.mode = USER_MODE;
        request = optarg;
        break;
      default:
        print_usage();
        exit(EXIT_FAILURE);
    }
  }

  // Check parameters.
  if (!(voithia.mode)) {
    fprintf(stderr, "Error: One of -g, -p, -o is required.\n\n");
    print_usage();
    exit(0);
  }
  if (!host) {
    fprintf(stderr, "Error: -a <address> is required.\n\n");
    print_usage();
    exit(0);
  }
  
  // get the host (server) info
  if ((host_info = gethostbyname(host)) == NULL) { 
    ERROR("gethostbyname()"); 
  }
    
  // create socket adress of server (type, IP-adress and port number)
  bzero(&voithia.server_addr, sizeof(voithia.server_addr)); //zero the rest of the struct
  voithia.server_addr.sin_family = AF_INET;
  voithia.server_addr.sin_addr = *((struct in_addr*)host_info->h_addr);
  voithia.server_addr.sin_port = htons(SERVER_PORT);

  if (voithia.mode == USER_MODE) {
    memset(voithia.snd_buffer, 0, BUF_SIZE);
    strncpy(voithia.snd_buffer, request, strlen(request));
    printf("Operation: %s\n", voithia.snd_buffer);
    talk(voithia.server_addr, voithia.snd_buffer);
  } else {
    while(--count>=0) { 
	  pthread_t *client_threads;
      client_threads=(pthread_t*)malloc(10*sizeof(pthread_t));
	  int i;
	  for(i=0; i<10; i++){ 
		  voithia.thread_number=i;
		  pthread_create(&client_threads[i],NULL,client_help,&voithia);
	  }
    }
  }
  return 0;
}

