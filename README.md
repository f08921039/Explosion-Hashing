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

Here is an overview of the project's layout:
.   
├── src/                # Source codes: .h and .c  
├── test/               # test prograims  
├── Makefile  
├── README.md

🔨 Build & Compilation

If your project requires compilation, use the commands below:
make

The default version is for 64-bit key and 64-bit value. 
If you want to compile the version supporting variable-length key and value, please remove -DDHT_INTEGER from makefile's CFLAGS.

All files in src will be compiled, obj directory will be created to contain .o file.
Then these objs compose libexplosion_hashing.a in lib directory.
Finally, the codes in test directory can be compiled and link explosion_hashing libray, generating executable prograims in the current path.

🚀 Execution (Running the App)
You can add your test code (e.g., test_app.c) in test directory, and add 'test_app' in makefile's TARGET.
After compiling, run your test prograim:
./test_app


🔌 API Documentation
All APIs and required structures are declared in src/dht.h ("dht" means dynamic hash table):
1. ....

    ...

2. ...

    ...
