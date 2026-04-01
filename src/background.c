#include "background.h"
#include "eh.h"

#define SPLIT_RR_RECORD_ENTS   32

#define SPLIT_RECHECK_LOOP_ENTS 4

#define GC_RR_RECORD_ENTS   32

#define CHUNK_GC_INCOMPLETE_THREHOLD    8

#define TRIGGER_SPLIT_ENTS_THREHOLD 64
#define TRIGGER_SPLIT_COUNT 32

typedef enum {
    URGENT_NUMA_ORDER = 0,
    URGENT_ORDER = 1, 
    EMERGRNCY_ORDER = 2, 
    FAST_EMERGRNCY_ORDER = 3, 
    FAST_URGENT_ORDER = 4, 
    HIGH_ORDER = 5, 
    FAST_HIGH_ORDER = 6, 
    NUMA_ORDER = 7, 
    LOW_ORDER = 8, 
    DEFER_NUMA_ORDER = 9, 
    DEFER_ORDER = 10, 
    FAST_NUMA_ORDER = 11, 
    FAST_LOW_ORDER = 12, 
    SPLIT_ORDER_NUM = 13
} SPLIT_ORDER;

typedef enum {
    URGENT_HELP_ORDER = 0, 
    HIGH_HELP_ORDER = 1,
    LOW_HELP_ORDER = 2, 
    NUMA_HELP_ORDER = 3, 
    HELP_ORDER_NUM = 4
} HELP_ORDER;

static const SPLIT_MAP split_map[SPLIT_MAP_NUM] = {
                            URGENT_NUMA_MAP, 
                            URGENT_MAP, 
                            EMERGENCY_MAP, 
                            FAST_EMERGENCY_MAP, 
                            FAST_URGENT_MAP,  
                            HIGH_MAP, 
                            FAST_HIGH_MAP, 
                            NUMA_MAP, 
                            LOW_MAP, 
                            DEFER_NUMA_MAP, 
                            DEFER_MAP, 
                            FAST_NUMA_MAP, 
                            FAST_LOW_MAP
};

static const HELP_MAP help_map[HELP_MAP_NUM] = {
                            HELP_URGENT_MAP, 
                            HELP_HIGH_MAP, 
                            HELP_LOW_MAP, 
                            HELP_NUMA_MAP
};

static const SPLIT_MAP help_map_split[HELP_MAP_NUM] = {
                            URGENT_MAP, 
                            HIGH_MAP, 
                            LOW_MAP, 
                            NUMA_MAP
};

static const HELP_MAP split_map_help[SPLIT_MAP_NUM] = {
                            HELP_MAP_NUM, 
                            HELP_URGENT_MAP, 
                            HELP_MAP_NUM, 
                            HELP_MAP_NUM, 
                            HELP_MAP_NUM, 
                            HELP_HIGH_MAP, 
                            HELP_MAP_NUM, 
                            HELP_NUMA_MAP, 
                            HELP_LOW_MAP, 
                            HELP_MAP_NUM, 
                            HELP_MAP_NUM, 
                            HELP_MAP_NUM, 
                            HELP_MAP_NUM
};


typedef enum {
    SPARSE_GC_ORDER = 0,
    DENSE_GC_ORDER = 1, 
    LOW_GC_ORDER = 2, 
    GC_ORDER_NUM = 3
} GC_ORDER;

static const GC_MAP gc_map[GC_MAP_NUM] = {
                            GC_SPARSE_MAP,
                            GC_DENSE_MAP, 
                            GC_LOW_MAP
};

static const GC_PRIORITY gc_prio_map[GC_MAP_NUM] = {
                            GC_SPARSE_PRIO,
                            GC_DENSE_PRIO, 
                            GC_LOW_PRIO
};

__attribute__((always_inline))
static void update_epoch_for_split(
                    struct tls_context *tls, 
                    struct tls_context *self) {
    unsigned long epoch;

    epoch = READ_ONCE(tls->epoch);

    if (epoch != MAX_LONG_INTEGER) {
        epoch += 100;

        if (epoch > self->epoch) {
            release_fence();
            WRITE_ONCE(self->epoch, epoch);
        }
    }
}


__attribute__((always_inline))
static SPLIT_ORDER check_higher_prio_split(
                struct tls_context *tls, 
                SPLIT_ORDER order) {
    RECORD_POINTER tail, head, *split_tail, *split_head;
    struct record_page *h_page, *t_page;
    unsigned long h_ent_num, t_ent_num;
    SPLIT_MAP map;
    SPLIT_ORDER check_order;

    for (check_order = URGENT_NUMA_ORDER; check_order < order; ++check_order) {
        map = split_map[check_order];

        split_head = &tls->split_rp[map];
        split_tail = &tls->split_rp_tail[map];

        head = READ_ONCE(*split_head);
        acquire_fence();

        tail = READ_ONCE(*split_tail);

        t_page = page_of_record_pointer(tail);
        t_ent_num = ent_num_of_record_pointer(tail);

        h_page = page_of_record_pointer(head);
        h_ent_num = ent_num_of_record_pointer(head);

        if (t_ent_num > h_ent_num + SPLIT_RECHECK_LOOP_ENTS || 
            (t_page != h_page && h_page->next != t_page) || 
            (h_page->next == t_page && t_ent_num + SPLIT_ENT_PER_RECORD_PAGE > 
                                            h_ent_num + SPLIT_RECHECK_LOOP_ENTS))
            break;
    }

    return check_order;
}

static int process_split_entry() {
    struct tls_context *tls, *self;
    RECORD_POINTER tail, head, new_head, help_rp;
    RECORD_POINTER *split_tail, *split_head;
    struct eh_split_context split_context;
    struct eh_split_entry *ent, *tail_ent;
    struct record_page *h_page, *t_page;
    SPLIT_ORDER order, check_order;
    int tid_begin, tid_end;
    int h_ent_num, t_ent_num, t, accum, ret;
    bool repeat, redo;
    SPLIT_MAP map;
    HELP_MAP h_map;

    self = tls_context_itself();
    
    tid_begin = self->tid_begin;
    tid_end = self->tid_end;
    
    order = URGENT_NUMA_ORDER;

    while (order < SPLIT_ORDER_NUM) {
        redo = repeat = false;

        for (t = tid_begin; t < tid_end + 1; ++t) {
            tls = (t != tid_end ? tls_context_of_tid(t) : self);

            accum = 0;
            map = split_map[order];

            split_head = &tls->split_rp[map];
            split_tail = &tls->split_rp_tail[map];

        tranverse_split_record_again :
            head = READ_ONCE(*split_head);
            acquire_fence();

            tail = READ_ONCE(*split_tail);
    
            t_page = page_of_record_pointer(tail);
            t_ent_num = ent_num_of_record_pointer(tail);

            h_page = page_of_record_pointer(head);
            h_ent_num = ent_num_of_record_pointer(head);

            tail_ent = &t_page->s_ent[t_ent_num];
            ent = &h_page->s_ent[h_ent_num];

            while (ent != tail_ent) {
                ret = analyze_eh_split_entry(ent, &split_context);

                if (unlikely(ret == 1))
                    break;

                if (ret == 0) {
                    if (split_context.type != INCOMPLETE_SPLIT)
                        accum += 1;

                    ret = eh_split(&split_context);

                    if (unlikely(ret == -1)) {
                        //to dooooooooo handle memory lack
                    }
                }

                if (h_ent_num != SPLIT_ENT_PER_RECORD_PAGE - 1)
                    h_ent_num += 1;
                else {
                    h_map = split_map_help[order];
                    
                    if (h_map < HELP_MAP_NUM) {
                        help_rp = READ_ONCE(tls->help_rp[h_map]);
                        
                        if (help_rp != INVALID_RECORD_POINTER && h_page == page_of_record_pointer(help_rp))
                            WRITE_ONCE(tls->help_rp[h_map], INVALID_RECORD_POINTER);
                    }

                    reclaim_concurrent_record_page(h_page);

                    h_ent_num = 0;
                    h_page = h_page->next;
                }

                if (accum > SPLIT_RR_RECORD_ENTS) {
                    repeat = true;
                    break;
                }

                ent = &h_page->s_ent[h_ent_num];
            }

            new_head = make_record_pointer(h_page, h_ent_num);

            if (unlikely(!cas_bool(split_head, head, new_head)))
                goto tranverse_split_record_again;

            update_epoch_for_split(tls, self);

            check_order = check_higher_prio_split(tls, order);

            if (unlikely(order != check_order)) {
                redo = true;

                order = check_order;
                break;
            }
        }

        if (redo)
            continue;

        if (!repeat)
            ++order;        
    }

    return 0;
}

static int process_chunk_gc_entry() {
    struct tls_context *tls, *self;
    RECORD_POINTER tail, head, new_head, help_rp;
    RECORD_POINTER *gc_tail, *gc_head;
    struct reclaim_entry *ent, *tail_ent;
    struct record_page *h_page, *t_page;
    GC_ORDER order;
    SPLIT_ORDER check_order;
    int tid_begin, tid_end;
    int h_ent_num, t_ent_num, t, accum, fail_accum, ret;
    bool repeat;
    GC_MAP map;

    self = tls_context_itself();

    tid_begin = self->tid_begin;
    tid_end = self->tid_end;
    
    order = SPARSE_GC_ORDER;

    while (order < GC_ORDER_NUM) {
        repeat = false;
        fail_accum = 0;

        for (t = tid_begin; t < tid_end + 1; ++t) {
            tls = (t != tid_end ? tls_context_of_tid(t) : self);

            accum = 0;
            map = gc_map[order];

            gc_head = &tls->gc_rp[map];
            gc_tail = &tls->gc_rp_tail[map];

        tranverse_gc_record_again :
            head = READ_ONCE(*gc_head);
            acquire_fence();

            tail = READ_ONCE(*gc_tail);
    
            t_page = page_of_record_pointer(tail);
            t_ent_num = ent_num_of_record_pointer(tail);

            h_page = page_of_record_pointer(head);
            h_ent_num = ent_num_of_record_pointer(head);

            tail_ent = &t_page->r_ent[t_ent_num];
            ent = &h_page->r_ent[h_ent_num];

            while (ent != tail_ent) {
                ret = eh_gc_kv(ent, gc_prio_map[order]);

                if (unlikely(ret == 1))
                    break;

                if (ret == 0)
                    accum += 1;
                else
                    fail_accum += 1;

                if (h_ent_num != RECLAIM_ENT_PER_RECORD_PAGE - 1)
                    h_ent_num += 1;
                else {
                    reclaim_concurrent_record_page(h_page);

                    h_ent_num = 0;
                    h_page = h_page->next;
                }

                if (accum > GC_RR_RECORD_ENTS || fail_accum > CHUNK_GC_INCOMPLETE_THREHOLD) {
                    repeat = true;
                    break;
                }

                ent = &h_page->r_ent[h_ent_num];
            }

            new_head = make_record_pointer(h_page, h_ent_num);

            if (unlikely(!cas_bool(gc_head, head, new_head)))
                goto tranverse_gc_record_again;

            update_epoch_for_split(tls, self);

            check_order = check_higher_prio_split(tls, FAST_NUMA_ORDER);

            if (fail_accum > CHUNK_GC_INCOMPLETE_THREHOLD || check_order != FAST_NUMA_ORDER)
                return 1;
        }

        if (!repeat)
            ++order;        
    }

    return 0;
}


void *split_task(void *parameter) {
    union split_task_parameter task_parameter;
    struct split_task_input input;
    struct per_node_context *per_nc;
    unsigned long time_limit, time;

    task_parameter.parameter = parameter;
    input = task_parameter.input;

    per_nc = &node_context.all_node_context[input.nid];

    init_tls_context(input.nid, per_nc->max_work_thread_num + input.tid);

    while (1) {
        time = sys_time_us();
        time_limit = time + BACKGROUND_SPLIT_PERIOD;

        if (process_split_entry())
            continue;

        time = sys_time_us();

        if (time < time_limit) {
            if (process_chunk_gc_entry())
                continue;

            time = sys_time_us();

            if (time < time_limit)
                sleep_us(time_limit - time);//usleep(time_limit - t);
        }
    }

}

void collect_all_thread_info() {
    struct per_node_context *per_nc;
    struct tls_context *tls, *tls_array;
    RECORD_POINTER rp, *page_rp;
    unsigned long ep;
    int t, n, nodes, total_threads;

    nodes = node_context.node_num;

    for (n = 0; n < nodes; ++n) {
        per_nc = &node_context.all_node_context[n];

        total_threads = per_nc->total_thread_num;

        if (total_threads == 0)
            continue;

        tls_array = per_nc->max_tls_context;

        page_rp = per_nc->page_rp;

        for (t = 0; t < total_threads; ++t) {
            tls = &tls_array[t];

            rp = READ_ONCE(tls->page_reclaim_tail);
            WRITE_ONCE(page_rp[t], rp);

            acquire_fence();

            ep = READ_ONCE(tls->epoch);

            if (ep == MAX_LONG_INTEGER)
                continue;

            if (ep > node_context.max_epoch)
                node_context.max_epoch = ep;

            if (ep < node_context.min_epoch)
                node_context.min_epoch = ep;

            if (ep + 1000 < node_context.epoch)
                WRITE_ONCE(tls->epoch, node_context.epoch - 1);
        }
    }
}

void free_reclaim_area(
            struct tls_context *tls, 
            RECORD_POINTER tail) {
    RECORD_POINTER *head_ptr, free_rp, rp;
    struct reclaim_entry *ent, *carve;
    struct record_page *h_page, *t_page, *page;
    void *reclaim_addr;
    int h_ent_num, t_ent_num, reclaim_size, min_free, min_resv, free_ents, t, work_thread_num;

    head_ptr = &tls->page_reclaim;

    h_page = page_of_record_pointer(*head_ptr);
    h_ent_num = ent_num_of_record_pointer(*head_ptr);

    t_page = page_of_record_pointer(tail);
    t_ent_num = ent_num_of_record_pointer(tail);

    carve = (struct reclaim_entry *)h_page->carve;

    ent = &h_page->r_ent[h_ent_num];

    work_thread_num = per_node_context->max_work_thread_num;
    t = 0;

    min_free = 0;
    min_resv = BACKGROUND_GC_PRESERVE_SEG;

    while (ent != carve) {
        reclaim_addr = page_reclaim_addr(ent);
        reclaim_size = page_reclaim_size(ent);

        if (reclaim_size != 1)
            free_page_aligned(reclaim_addr, reclaim_size);
        else {
            /*if (min_free) {
                --min_free;
                free_page_aligned(reclaim_addr, EH_SEGMENT_SIZE);
            } else {*/
                tls = tls_context_of_tid(t);

                free_rp = READ_ONCE(tls->seg_free);

                if (free_rp != INVALID_RECORD_POINTER) {
                    page = page_of_record_pointer(free_rp);
                    free_ents = ent_num_of_record_pointer(free_rp);

                    if (free_ents < BACKGROUND_GC_PRESERVE_SEG) {
                        if (free_ents < min_resv)
                            min_resv = free_ents;

                        *(struct record_page **)reclaim_addr = page;
                        rp = make_record_pointer((struct record_page *)reclaim_addr, free_ents + 1);

                        free_rp = cas(&tls->seg_free, free_rp, rp);
                    } else
                        free_page_aligned(reclaim_addr, EH_SEGMENT_SIZE);
                }

                if (free_rp == INVALID_RECORD_POINTER) {
                    min_resv = 0;
                    rp = make_record_pointer((struct record_page *)reclaim_addr, 1);
                    WRITE_ONCE(tls->seg_free, rp);
                }

                if (++t == work_thread_num) {
                    t = 0;
                    
                    if (min_resv > BACKGROUND_GC_PRESERVE_SEG)
                        min_free = min_resv;
                    
                    min_resv = BACKGROUND_GC_PRESERVE_SEG;
                }
            //}
        }

        if (h_ent_num != RECLAIM_ENT_PER_RECORD_PAGE - 1)
            h_ent_num += 1;
        else {
            h_ent_num = 0;
            page = h_page->next;
            free_page_aligned(h_page, RECORD_PAGE_SIZE);
            h_page = page;
        }

        ent = &h_page->r_ent[h_ent_num];
    }

    h_page->carve = (void *)&t_page->r_ent[t_ent_num];

    *head_ptr = make_record_pointer(h_page, h_ent_num);
}

__attribute__((always_inline))
static void free_all_reclaim_page() {
    struct tls_context *tls;
    RECORD_POINTER rp, *page_rp;
    int t, total_threads;

    total_threads = per_node_context->total_thread_num;

    if (total_threads > 0) {
        page_rp = per_node_context->page_rp;

        for (t = 0; t < total_threads; ++t) {
            tls = tls_context_of_tid(t);
            rp = READ_ONCE(page_rp[t]);
            
            if (likely(rp != INVALID_RECORD_POINTER))
                free_reclaim_area(tls, rp);
        }
    }
}

/*__attribute__((always_inline))
static void free_all_reclaim_chunk() {
    struct tls_context *tls;
    RECORD_POINTER rp, *chunk_rp;
    int t, total_threads;

    total_threads = per_node_context->total_thread_num;

    if (total_threads > 0) {
        chunk_rp = per_node_context->chunk_rp;

        for (t = 0; t < total_threads; ++t) {
            tls = tls_context_of_tid(t);
            rp = READ_ONCE(chunk_rp[t]);
            
            if (likely(rp != INVALID_RECORD_POINTER))
                free_reclaim_area(tls, rp, false);
        }
    }
}*/

__attribute__((always_inline))
int check_gc_version() {
    struct per_node_context *per_nc;
    int nodes, n;

    nodes = node_context.node_num;

    for (n = 0; n < nodes; ++n) {
        per_nc = &node_context.all_node_context[n];

        if (per_nc->total_thread_num == 0)
            continue;

        if (node_context.gc_version != READ_ONCE(per_nc->gc_version))
            return -1;
    }

    return 0;
}


void *gc_task(void *parameter) {
    u64 time_limit, t;

    node_id = (int)parameter;
    per_node_context = &node_context.all_node_context[node_id];
    tls_context_array = per_node_context->max_tls_context;
    
    if (per_node_context->gc_main) {
        node_context.epoch = 0;
        node_context.max_epoch = 0;
        node_context.min_epoch = MAX_LONG_INTEGER;
    }


    while (1) {
        time_limit = sys_time_us() + BACKGROUND_GC_PERIOD;

        if (per_node_context->gc_main) {
            node_context.min_epoch = MAX_LONG_INTEGER;
            collect_all_thread_info();

            if (unlikely(node_context.min_epoch == MAX_LONG_INTEGER)) {
                sleep_us(BACKGROUND_GC_PERIOD);//usleep(BACKGROUND_GC_PERIOD);
                continue;
            }
        }

        if (per_node_context->gc_main && 
                node_context.epoch < node_context.min_epoch
                                    && check_gc_version() == 0) {
            node_context.epoch = node_context.max_epoch;

            release_fence();
            WRITE_ONCE(node_context.gc_version, node_context.gc_version + 1);
        }

        if (per_node_context->gc_version != READ_ONCE(node_context.gc_version)) {
            free_all_reclaim_page();
            //free_all_reclaim_chunk();

            release_fence();
            WRITE_ONCE(per_node_context->gc_version, node_context.gc_version);
        }

        t = sys_time_us();

        if (t > time_limit)
            continue;

        sleep_us(time_limit - t);
    }
}

__attribute__((always_inline))
static int prefault_memory(int tid_begin, int tid_end) {
    int t, ret;

    ret = 0;

    for (t = tid_begin; t < tid_end; ++t)
        if (prefault_eh_seg(t))
            ret = 1;

    if (prefault_eh_seg(tls_context_id))
        ret = 1;

    return ret;
}

__attribute__((always_inline))
static int trigger_memory(int tid_begin, int tid_end) {
    struct tls_context *tls;
    struct record_page *t_page, *h_page;
    RECORD_POINTER rp, tail, head;
    struct eh_split_entry *ent, *tail_ent;
    unsigned long t_ent_count, h_ent_count;
    HELP_ORDER order;
    HELP_MAP h_map;
    SPLIT_MAP h_map_s;
    int count, t, t_ent_num, h_ent_num, redo, ret;

    redo = 0;

    for (t = tid_begin; t < tid_end; ++t) {
        tls = tls_context_of_tid(t);
        
        for (order = URGENT_HELP_ORDER; order < HELP_ORDER_NUM; ++order) {
            h_map = help_map[order];
            h_map_s = help_map_split[order];
            
            rp = READ_ONCE(tls->help_rp[h_map]);
            
            head = READ_ONCE(tls->split_rp[h_map_s]);
            acquire_fence();
            tail = READ_ONCE(tls->split_rp_tail[h_map_s]);
            
            t_page = page_of_record_pointer(tail);
            t_ent_num = ent_num_of_record_pointer(tail);
            
            if (rp == INVALID_RECORD_POINTER) {
                h_page = page_of_record_pointer(head);
                h_ent_num = ent_num_of_record_pointer(head);
            
                t_ent_count = t_page->counter + t_ent_num;
                h_ent_count = h_page->counter + h_ent_num;

                if (t_ent_count < h_ent_count + TRIGGER_SPLIT_ENTS_THREHOLD)
                    continue;

                rp = head;
                WRITE_ONCE(tls->help_rp[h_map], rp);
            } else {
                h_page = page_of_record_pointer(rp);
                h_ent_num = ent_num_of_record_pointer(rp);
            }
        
            tail_ent = &t_page->s_ent[t_ent_num];
            ent = &h_page->s_ent[h_ent_num];
            
            count = 0;

            while (ent != tail_ent && count < TRIGGER_SPLIT_COUNT) {
                ret = trigger_eh_seg(ent);

                if (unlikely(ret == 1))
                    break;
                    
                if (h_ent_num != SPLIT_ENT_PER_RECORD_PAGE - 1)
                    h_ent_num += 1;
                else {
                    h_ent_num = 0;
                    h_page = h_page->next;
                }

                ent = &h_page->s_ent[h_ent_num];
                
                head = make_record_pointer(h_page, h_ent_num);

                if (!cas_bool(&tls->help_rp[h_map], rp, head))
                    break;
                    
                rp = head;
                
                if (ret != -1)
                    ++count;
            }
            
            if (count == TRIGGER_SPLIT_COUNT)
                redo = 1;
        }
        
    }

    return redo;
}

void *prefault_task(void *parameter) {
    u64 time_limit, t;
    union split_task_parameter task_parameter;
    struct split_task_input input;
    int tid_begin, tid_end, repeat_1, repeat_2;

    task_parameter.parameter = parameter;
    input = task_parameter.input;

    per_node_context = &node_context.all_node_context[input.nid];
    
    node_id = input.nid;
    tls_context_id = per_node_context->max_work_thread_num + input.tid;
    
    tls_context_array = per_node_context->max_tls_context;
    tls_context = tls_context_of_tid(tls_context_id);
    
    tid_begin = input.tid * THREADS_PER_SPLIT_THREAD;
    tid_end = tid_begin + THREADS_PER_SPLIT_THREAD;

    if (tid_end > per_node_context->max_work_thread_num)
        tid_end = per_node_context->max_work_thread_num;

    while (1) {
        time_limit = sys_time_us() + BACKGROUND_PREFAULT_PERIOD;

        repeat_1 = prefault_memory(tid_begin, tid_end);
        repeat_2 = trigger_memory(tid_begin, tid_end);

        t = sys_time_us();

        if (repeat_1 || repeat_2 || t > time_limit)
            continue;

        sleep_us(time_limit - t);
    }
}
