/*
 * par_sumsq.c
 *
 * CS 446.646 Project 1 (Pthreads)
 *
 * Compile with --std=c99
 */

#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

// aggregate variables
long sum = 0;
long odd = 0;
long min = INT_MAX;
long max = INT_MIN;
bool future_tasks = true;

// task queue structs
typedef struct task_node {
	long num;
	struct task_node* next;
} task_node_t;

typedef struct task_queue {
	task_node_t* head;
	task_node_t** tail;
} task_queue_t;

// task queue
task_queue_t queue = { NULL, NULL };

// function prototypes
void calculate_square(long number);
void* thread_routine(void* arg);
// may need to return int or bool in case of error
void add_task(long num);
// assumes queue is nonempty
long remove_task();

// mutex locks + condition variable
pthread_mutex_t queue_lock;
pthread_mutex_t globals_lock;
pthread_cond_t  not_empty;


int main(int argc, char* argv[])
{
  // check and parse command line options
  if (argc != 3) {
    printf("Error: incorrect # of arguments\n");
    exit(EXIT_FAILURE);
  }
  if (atoi(argv[2]) <= 0) {
    printf("Error: non-positive worker #\n");
    exit(EXIT_FAILURE);
  }
  
  // generate workers
  int num_workers = atoi(argv[2]);
  pthread_t workers[num_workers];
  for (int i = 0; i < num_workers; i++) {
    printf("Master: creating thread %d\n", i);
    pthread_create(&workers[i], NULL, thread_routine, (void*) i);
  }  
  
  // initialize mutex locks
  pthread_mutex_init(&queue_lock, NULL);
  pthread_mutex_init(&globals_lock, NULL);
  
  // initialize condition variable
  pthread_cond_init(&not_empty, NULL);
  
  // load numbers and add them to the queue
  char *fn = argv[1];
  FILE* fin = fopen(fn, "r");
  char action;
  long num;

  while (fscanf(fin, "%c %ld\n", &action, &num) == 2) {
    if (action == 'p') {            // add action to task queue
      pthread_mutex_lock(&queue_lock);
      // debug
      printf("Master: adding task: %ld\n", num);
      add_task(num);
      pthread_cond_broadcast(&not_empty);
      pthread_mutex_unlock(&queue_lock);
    } else if (action == 'w') {     // wait, nothing new happening
      sleep(num);
    } else {
      printf("ERROR: Unrecognized action: '%c'\n", action);
      exit(EXIT_FAILURE);
    }
  }
  fclose(fin);
  
  // file is empty; there will be no more future tasks
  pthread_mutex_lock(&queue_lock);
  // debug
  printf("Master: no more tasks\n");
  future_tasks = false;
  pthread_mutex_unlock(&queue_lock);

  // wait for current threads to finish processing
  // debug
  printf("Master: waiting for threads to finish\n");
  for (int i = 0; i < num_workers; i++)
    pthread_join(workers[i], NULL);
  
  // print results
  printf("%ld %ld %ld %ld\n", sum, odd, min, max);
  
  // clean up and return
  return (EXIT_SUCCESS);
}

void* thread_routine(void* arg) {
  bool no_work_left = false;
  while (true) {
    // gain access to queue
    pthread_mutex_lock(&queue_lock);

    // debug
    printf("Thread %d waiting\n", (int)arg);

    // wait for queue to be populated
    while (queue.head == NULL) {
      // if we know the queue will never be populated, then we will not wait
      if (!future_tasks) {
        no_work_left = true;
        break;
      }
      pthread_cond_wait(&not_empty, &queue_lock);
    }
    
    // if there is no work to do, we can end
    if (no_work_left) {
      pthread_mutex_unlock(&queue_lock);
      break;
    }
    
    // queue has been populated and control has been given to us; grab value
    long num = remove_task();

    // debug
    printf("Thread %d processing value %ld\n", (int)arg, num);

    // queue access is finished, return control
    pthread_mutex_unlock(&queue_lock);

    // process number and update globals
    calculate_square(num);
    
  }
  // debug
  printf("Thread %d exiting\n", (int)arg);
  return arg;
}

/*
 * update global aggregate variables given a number
 */
void calculate_square(long number)
{

  // calculate the square
  long the_square = number * number;

  // ok that was not so hard, but let's pretend it was
  // simulate how hard it is to square this number!
  sleep(number);

  // access globals
  pthread_mutex_lock(&globals_lock);

  // let's add this to our (global) sum
  sum += the_square;

  // now we also tabulate some (meaningless) statistics
  if (number % 2 == 1) {
    // how many of our numbers were odd?
    odd++;
  }

  // what was the smallest one we had to deal with?
  if (number < min) {
    min = number;
  }

  // and what was the biggest one?
  if (number > max) {
    max = number;
  }

  // return control of globals
  pthread_mutex_unlock(&globals_lock);
}

/*
 * adds task to queue
 */
void add_task(long num) {
  task_node_t* new_node;

  // allocate memory
  new_node = malloc(sizeof(task_node_t));

  // error check
  if (new_node == NULL) {
    printf("Error: malloc error\n");
  }

  // put values into node
  new_node->num  = num;
  new_node->next = NULL;

  // determine where to put the node
  if (queue.head == NULL) {
    queue.head = new_node;
  } else {
    *queue.tail = new_node;
  }

  // set new tail
  queue.tail = &new_node->next;
}

/*
 * removes from front of queue
 */
long remove_task() {
  long num;
  task_node_t* front = queue.head;
  queue.head = queue.head->next;
  num = front->num;
  free(front);
  return num;
}
