# Explosion-Hashing

ExpHash

A hased-based in-memory key-value store delivers superior QoS—characterized by high throughput, low latency, and robust stability.

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

To add a test, place your source (e.g., test_app.c) in the test/ directory and add the name to TARGET in the Makefile.  
After compiling, run your test prograim:  
``` ./test_app ```


🔌 API Documentation  

All APIs and required structures are declared in src/dht.h ("dht" means dynamic hash table):
1. ....

    ...

2. ...

    ...
