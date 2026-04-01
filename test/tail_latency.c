
#include "dht.h"
#include <stdio.h>

#define NUMA_NUM	2
#define WT_NUM_PER_NUMA	36
#define MAX_WT_NUM_PER_NUMA	36


#define INSERT_NUM  29257143UL

#define TAIL10_NUM 10

static unsigned int time_record[WT_NUM_PER_NUMA*NUMA_NUM][1000000];

void *func(void *para) {
	u64 i, ent = INSERT_NUM;
	u64 val;
	u64 t1, t2;
	int ind = (int)para;

	for (i = ent * ind; i < ent * (ind + 1); ++i) {
	        t1 = sys_time_ns();
	        
		if (dht_kv_insert(i, i)) {
			printf("dht_kv_insert %lu failed\n", i);
			return NULL;
		}
		
		t2 = sys_time_ns();
		
		if (t2 - t1 >= 1000000)
		    time_record[ind][1000000 - 1] += 1;
		else
		    time_record[ind][t2 - t1] += 1;
	}


	while (1) {sleep_us(1000000000);}
}


int main() {
	struct dht_node_context n_context;
	int max_node_thread[NUMA_NUM];
	int node_thread[NUMA_NUM];
	struct dht_work_function *work_func[NUMA_NUM];
	struct dht_work_function work_func_arr[NUMA_NUM][MAX_WT_NUM_PER_NUMA];

	int n, i, j, ret;
	unsigned long sum, count;
	unsigned int tail[TAIL10_NUM];
	
	memset(&time_record[0][0], 0, (WT_NUM_PER_NUMA*NUMA_NUM*1000000) / sizeof(unsigned int));

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
	
	sleep_us(60000000);
	
	for (i = 1; i < WT_NUM_PER_NUMA*NUMA_NUM; ++i) {
	    for (j = 0; j < 1000000; ++j)
	        time_record[0][j] += time_record[i][j];
	}
	
	
	sum = 0;
	count = WT_NUM_PER_NUMA*NUMA_NUM*INSERT_NUM;
	
	i = TAIL10_NUM;
	
	while (i) {
	    count -= 92160000;//count /= 10;
	    if (count == 0)
	        count = 1;

	    tail[--i] = count;
	}
	
	for (j = 0; j < 1000000; ++j) {
	    sum += time_record[0][1000000 - 1 - j];
	
	    while (sum >= tail[i]) {
	        printf("%u %d\n", tail[i], 1000000 - 1 - j);
	        if (++i == TAIL10_NUM) {
	            j = 1000000;
	            break;
	        }
	    }
	}

	while (1) {}
	return 0;
}
