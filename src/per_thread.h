#ifndef __PER_THREAD_H
#define __PER_THREAD_H

#include "compiler.h"
#include "kv.h"

#include "eh.h"

#ifdef  __cplusplus
extern  "C" {
#endif

#define THREADS_PER_SPLIT_THREAD    3


struct thread_paramater {
    int thread_id;
    int node_id;
    pthread_t work_pthread_id;
    pthread_t split_pthread_id;
    pthread_t prefault_pthread_id;
    void *(*callback_fuction)(void *);
    void *paramater;
};

struct per_node_context {
    int max_work_thread_num;
    int work_thread_num;
    int split_thread_num;
    int total_thread_num;
    unsigned char gc_enable;
    unsigned char gc_main;
    unsigned short padding;//unsigned short global_depth;
    int gc_version;
    pthread_t gc_pthread;
    struct tls_context *max_tls_context;
    struct thread_paramater *thread_paramater;
    RECORD_POINTER *chunk_rp;//to dooooo eliminate
    RECORD_POINTER *page_rp;
};

struct node_context {
    short node_num;
    short actual_node_num;
    int gc_version;
    u64 epoch;
    u64 max_epoch;
    u64 min_epoch;
    struct per_node_context *all_node_context;
};

typedef enum {
	URGENT_NUMA_MAP = 0, 
    INCOMPLETE_MAP = URGENT_NUMA_MAP, 
    URGENT_MAP = 4, 
    FAST_URGENT_MAP = 5, 
    EMERGENCY_MAP = 6, 
    FAST_EMERGENCY_MAP = 7, 
    NUMA_MAP = 1, 
    FAST_NUMA_MAP = 2, 
	HIGH_MAP = 8, 
    FAST_HIGH_MAP = 9, 
	LOW_MAP = 10, 
    FAST_LOW_MAP = 11, 
    DEFER_NUMA_MAP = 3, 
    DEFER_MAP = 12, 
    SPLIT_MAP_NUM = 13
} SPLIT_MAP;

typedef enum {
	GC_SPARSE_MAP = 0, 
    GC_DENSE_MAP = 1, 
    GC_LOW_MAP = 2,  
    GC_MAP_NUM = 3
} GC_MAP;

typedef enum {
    HELP_URGENT_MAP = 0, 
    HELP_HIGH_MAP = 1, 
    HELP_LOW_MAP = 2, 
    HELP_NUMA_MAP = 3, 
    HELP_MAP_NUM = 4
} HELP_MAP;

struct tls_context {
    void *seg_prefault;
    unsigned long seg_prefault_count;
    union {
        struct {
            //unsigned long splits_statis;
            //unsigned long splited_statis;
        };
        struct {
            PAGE_POOL numa_seg_pool;
            PAGE_POOL numa_extra_seg;
            PAGE_POOL numa_record_pool;
        };
    };
    RECORD_POINTER seg_free_self;
    RECORD_POINTER seg_free;
    RECORD_POINTER page_reclaim;
    RECORD_POINTER help_rp[HELP_MAP_NUM];
    RECORD_POINTER split_rp[SPLIT_MAP_NUM];
    RECORD_POINTER split_rp_tail[SPLIT_MAP_NUM];
    RECORD_POINTER gc_rp[GC_MAP_NUM];
    RECORD_POINTER gc_rp_tail[GC_MAP_NUM];
    RECORD_POINTER page_reclaim_tail;
    PAGE_POOL seg_pool;
    PAGE_POOL seg_backup;
    PAGE_POOL record_pool;
    unsigned long seg_prefault_hint;
    struct tls_context *background_tls;
    int tid_begin;
    int tid_end;
    //struct eh_gc_context thread_gc_context;
    u64 padding[5];

    union {
        struct eh_split_context thread_split_context;
        struct {
            struct tls_context *tls_begin;
            int rr_tid;
        };
    };
    void *chunk_alloc_hot;
    void *chunk_alloc;
    unsigned long epoch;
};


extern __thread struct tls_context *tls_context;
extern __thread struct tls_context *tls_context_array;
extern __thread struct per_node_context *per_node_context;
extern __thread int node_id;
extern __thread int tls_context_id;

extern struct node_context node_context;


static inline 
struct tls_context *tls_context_itself() {
    return tls_context;
}

static inline 
struct tls_context *tls_context_of_tid(int tid) {
    return &tls_context_array[tid];
}

static inline 
int tid_itself() {
    return tls_context_id;
}

static inline 
int tls_node_id() {
    return node_id;
}

static inline 
struct per_node_context *tls_node_context() {
    return per_node_context;
}


static inline 
struct eh_split_context *tls_split_context() {
    return &tls_context->thread_split_context;
}

static inline 
struct eh_split_entry *tls_split_entry() {
    struct eh_split_context *split_context;

    split_context = tls_split_context();

    return &split_context->entry;
}

static inline 
SPLIT_STATE tls_split_state() {
    struct eh_split_context *split_context;
    SPLIT_STATE state;

    split_context = tls_split_context();

    state = READ_ONCE(split_context->state);

    return state;
}

/*static inline 
void tls_init_tid_range(int tid_begin, int tid_end) {
    tls_context->tid_begin = tid_begin;
    tls_context->tid_end = tid_end;
    tls_context->rr_tid = tid_begin;
}*/

/*static inline 
void increase_tls_splited() {
    unsigned long splited;

    splited = tls_context->splited;

    WRITE_ONCE(tls_context->splited, splited + 1);
}*/

static inline 
int tls_split_record_num(
          struct tls_context *tls, 
          SPLIT_MAP map) {
    RECORD_POINTER head, tail, *split_tail, *split_head;
    struct record_page *t_page, *h_page;
    unsigned long t_ent_num, h_ent_num;

    split_head = &tls->split_rp[map];
    split_tail = &tls->split_rp_tail[map];
    
    head = READ_ONCE(*split_head);
    acquire_fence();

    tail = READ_ONCE(*split_tail);
        
    h_page = page_of_record_pointer(head);
    h_ent_num = h_page->counter + ent_num_of_record_pointer(head);

    t_page = page_of_record_pointer(tail);
    t_ent_num = t_page->counter + ent_num_of_record_pointer(tail);
    
    return (t_ent_num - h_ent_num);
}


static inline 
bool is_tls_help_split() {
    struct eh_split_entry *s_ent;
    uintptr_t target_ent;

    s_ent = tls_split_entry();

    target_ent = READ_ONCE(s_ent->target);

    return (target_ent != INVALID_EH_SPLIT_TARGET);
}

void retrieve_tls_help_split();

void modify_tls_split_entry(
                    struct eh_segment *target_seg, 
                    struct eh_segment *dest_seg);

static inline 
void inform_tls_split_target(struct eh_segment *target_seg) {
    struct eh_split_context *s_context;
    uintptr_t target_ent;

    s_context = tls_split_context();

    target_ent = make_eh_split_target_entry(target_seg, 0, 0, INVALID_SPLIT);
	WRITE_ONCE(s_context->entry.target, target_ent);
}

static inline 
void confirm_tls_split_target(
            struct eh_segment *target_seg, 
            SPLIT_TYPE type) {
    struct eh_split_context *s_context;
    uintptr_t target_ent;

    s_context = tls_split_context();

    target_ent = make_eh_split_target_entry(target_seg, 0, 0, type);

    release_fence();
	WRITE_ONCE(s_context->entry.target, target_ent);
}

static inline 
void init_tls_split_entry(
			struct eh_segment *target_seg, 
			struct eh_segment *dest_seg, 
			u64 hashed_key, 
            int depth, 
            SPLIT_TYPE type) {
    struct eh_split_context *s_context;
    
    s_context = tls_split_context();

    s_context->hashed_prefix = hashed_key & MASK(PREHASH_KEY_BITS - depth);
    s_context->depth = depth;
    s_context->bucket_id = 0;
    s_context->dest_seg = dest_seg;
    s_context->inter_seg = NULL;
    s_context->type = type;

    confirm_tls_split_target(target_seg, type);
}

static inline
struct eh_split_context *possess_tls_split_context() {
    struct eh_split_context *s_context;
    struct eh_split_entry *s_ent;
    uintptr_t target, possess_target;
    struct eh_segment *target_seg;

    s_context = tls_split_context();
    s_ent = &s_context->entry;

    target = READ_ONCE(s_ent->target);

    if (target == INVALID_EH_SPLIT_TARGET)
        return NULL;

    target_seg = (struct eh_segment *)(target & EH_SPLIT_TARGET_SEG_MASK);
    possess_target = make_eh_split_target_entry(target_seg, 0, 0, INVALID_SPLIT);

    if (unlikely(!cas_bool(&s_ent->target, target, possess_target)))
        return NULL;

    return s_context;
}


static inline
int dispossess_tls_split_context() {
    struct eh_split_context *s_context;
    EH_BUCKET_HEADER header, ori_header;
    struct eh_segment *dest_seg, *target_seg;
    SPLIT_TYPE type;

    s_context = tls_split_context();

    dest_seg = s_context->dest_seg;
    target_seg = s_context->target_seg;
    type = s_context->type;

    confirm_tls_split_target(target_seg, type);

    memory_fence();
    header = READ_ONCE(dest_seg->bucket[0].header);

    if (unlikely(eh_seg_low(header)) || 
                unlikely(type == URGENT_SPLIT && 
                            header == INITIAL_EH_BUCKET_TOP_HEADER)) {
        if (type == NORMAL_SPLIT)
            dest_seg = eh_next_high_seg(header);
        else {
            ori_header = set_eh_split_entry(&s_context->entry, THREAD2_PRIO);
            cas(&dest_seg[2].bucket[0].header, ori_header, INITIAL_EH_BUCKET_TOP_HEADER);
        }

        modify_tls_split_entry(target_seg, dest_seg);
    }

    return 0;
}

static inline 
unsigned long get_epoch_per_thread() {
    return tls_context->epoch;
}

static inline 
void inc_epoch_per_thread() {
    release_fence();
    WRITE_ONCE(tls_context->epoch, tls_context->epoch + 1);
}

int prepare_all_tls_context();

void release_all_tls_context();

void init_tls_context(int nid, int tid);


struct eh_split_entry *new_split_record(
                            SPLIT_PRIORITY prio, 
                            bool fast);

struct eh_split_entry *new_other_split_record(
                            SPLIT_PRIORITY prio, 
                            int other_nid, 
                            bool fast);
                            
struct eh_split_entry *new_per_split_record(
                            SPLIT_PRIORITY prio, 
                            bool fast);

struct reclaim_entry *new_gc_record(GC_PRIORITY prio);

int reclaim_page(void *addr, int shift);

static inline 
void reclaim_concurrent_record_page(struct record_page *page) {
    unsigned long counter;

    counter = READ_ONCE(page->counter);

    if (counter != MAX_LONG_INTEGER && 
                cas_bool(&page->counter, counter, MAX_LONG_INTEGER))
        reclaim_page(page, RECORD_PAGE_SHIFT);
}


#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__PER_THREAD_H
