
#include "dht.h"
#include <stdio.h>
#include <stdint.h>
#include <math.h>

#define NUMA_NUM	2
#define WT_NUM_PER_NUMA	36
#define MAX_WT_NUM_PER_NUMA	36

static long zipf_arr[25600000UL*72];

typedef struct {
    uint64_t n;         // Number of items
    double theta;       // Skewness
    double alpha;       // 1 / (1 - theta)
    double zetan;       // Zeta(n, theta)
    double eta;         // (1 - pow(2.0/n, 1 - theta)) / (1 - zeta(2, theta)/zetan)
    double zeta2theta;  // Zeta(2, theta)
} ZipfianGen;

// Approximate generalized harmonic number for large n
double zeta_approx(uint64_t n, double theta) {
    return (pow(n, 1.0 - theta) - 1.0) / (1.0 - theta);
}

void zipf_init(ZipfianGen *z, uint64_t n, double theta) {
    z->n = n;
    z->theta = theta;
    z->alpha = 1.0 / (1.0 - theta);
    
    // We use a simplified zeta for the full range
    z->zetan = zeta_approx(n, theta);
    z->zeta2theta = 1.0 + pow(0.5, theta); 
    
    z->eta = (1.0 - pow(2.0 / n, 1.0 - theta)) / (1.0 - z->zeta2theta / z->zetan);
}

uint64_t zipf_next(ZipfianGen *z) {
    // Basic fast PRNG: Replace with xorshift64 if speed is critical
    double u = (double)rand() / (double)RAND_MAX;
    double uz = u * z->zetan;

    if (uz < 1.0) return 0;
    if (uz < 1.0 + pow(0.5, z->theta)) return 1;

    // The inversion: maps a uniform [0,1] to the Zipfian curve
    uint64_t val = (uint64_t)((double)z->n * pow(z->eta * u - z->eta + 1.0, z->alpha));
    
    // Bound check
    return (val >= z->n) ? z->n - 1 : val;
}

static int waiting = 0;

void *func(void *para) {
	u64 i, ent = 25600000;
	u64 val;
	u64 t1, t2;
	int ind = (int)para;
	
	printf("thread %d generate datasets\n", ind);

	for (i = ent * ind; i < ent * (ind + 1); ++i) {
		if (dht_kv_insert(i, i)) {
			printf("dht_kv_insert %lu failed\n", i);
			return NULL;
		}
	}
	
	sleep_us(50000000);
	
	atomic_add(&waiting, 1);
	
	while (READ_ONCE(waiting) != 72) {}
	
	printf("thread %d start zipfain lookups\n", ind);

	t2 = sys_time_us();

	for (i = ent * (ind); i < ent * (ind + 1); ++i)
	        dht_kv_lookup(i, &val);

	t1 = sys_time_us();

	printf("thread %d lookups %lu kvs: %lu us\n", ind, ent, t1 - t2);

	while (1) {sleep_us(1000000000);}
}


int main() {
	struct dht_node_context n_context;
	int max_node_thread[NUMA_NUM];
	int node_thread[NUMA_NUM];
	struct dht_work_function *work_func[NUMA_NUM];
	struct dht_work_function work_func_arr[NUMA_NUM][MAX_WT_NUM_PER_NUMA];

	int n, i, ret;
	u64 k;
	
	ZipfianGen z;
    
        zipf_init(&z, 25600000UL*72, 0.99);
        
        for (k = 0; k < 25600000UL*72; ++k)
                zipf_arr[k] = zipf_next(&z);

	for (n = 0; n < NUMA_NUM; ++n) {
		max_node_thread[n] = MAX_WT_NUM_PER_NUMA;
		node_thread[n] = WT_NUM_PER_NUMA;
                work_func[n] = &work_func_arr[n][0];

		for (i = 0; i < WT_NUM_PER_NUMA; ++i) {
			work_func_arr[n][i].start_routine = &func;
			work_func_arr[n][i].arg = (void *)(n * WT_NUM_PER_NUMA + i);
		}
	}

	n_context.nodes = NUMA_NUM;
	n_context.max_node_thread = &max_node_thread[0];

	ret = dht_init_structure(&n_context);
	
	if (ret) {
		printf("dht_init_structure failed\n");
		return -1;
	}

	n_context.node_thread = &node_thread[0];
	n_context.node_func = &work_func[0];

	ret = dht_create_thread(&n_context);

	if (ret) {
		printf("dht_create_thread failed\n");
		return -1;
	}

	while (1) {sleep_us(1000000000);}
	return 0;
}
