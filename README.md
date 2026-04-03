# Explosion-Hashing

ExpHash

A hased-based in-memory key-value store library delivers superior QoS—characterized by high throughput, low latency, and robust stability.

📌 Features

    Insert-biased: Optimized for high-performance handling of insert-intensive workloads.

    Massive Data Capacity: Maintains exceptional Quality of Service (QoS), even when managing billion-scale datasets.

    High Scalability: Throughput scales linearly with the increase of high-concurrency CPU cores.

🛠 Prerequisites

Before you begin, ensure you have met the following requirements:

    HARDWARE: 72 cores, 256GB DRAM (if lacking sufficient CPU and memory resources, you cannot run billion-sacle workloads) 

    OS: Linux kernel 6.14 (make sure your kernel provides MADV_POPULATE_WRITE and MADV_POPULATE_READ options for madvise syscall)

    COMPILER: gcc 13.3.0
    
    Library: pthread

    PACKAGE: libnuma-dev or other numa tools

📂 Directory Structure
 
.   
├── src/                # Source codes: .h and .c  
├── test/               # test prograims  
├── Makefile  
├── README.md

    
🔨 Build & Compilation

    Default: 64-bit key/value support.
    
    Variable-length key/value: Remove -DDHT_INTEGER from CFLAGS in the Makefile.

    Build Process: compile src/*.c into obj/ -> bundle .o into lib/libexplosion_hashing.a -> links .a to test/*.c -> generate exe.


If your project requires compilation, use the commands below:  
``` make ```   

🚀 Execution (Running the App)  


    Add a test: place your code (e.g., test_app.c) in the test/ , add the name to TARGET in the Makefile.
  
After compiling, run your test prograim (e.g., test_app):  
``` ./test_app ```


🔌 API Documentation  

All APIs and required structures are declared in src/dht.h ("dht" means dynamic hash table):
1. **struct dht_work_function**: a discriptor of an array of callback functions and their parameters.
```
    void *(*start_routine)(void *): a start pointer of your callback function array.  

    void *arg: a start pointer of the parameter array corresponding to callback functions.
``` 
2. **struct dht_node_context**: a discriptor for thread allocation across NUMA nodes.
```
    int nodes: the number of NUMA nodes in your system.  

    int *max_node_thread: a start pointer of the array recording the max thread count in the corresponding node.
    
    int *node_thread: a start pointer of the array recording the thread count expected to use in the corresponding node.  

    struct dht_work_function **node_func: a start pointer of dht_work_function array, whose element's callback functions will be pinned to the corresponding node.
```   
3. **int dht_init_structure(struct dht_node_context *node_context)**: Call this function to init per-node and per-thread data structures before using other APIs. If success, return 0; otherwise, return -1;
```
struct dht_node_context *node_context : nodes and max_node_thread feild should be assigned before passing this function.
```
4. 

    ...
