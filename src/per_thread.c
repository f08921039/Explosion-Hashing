#include "per_thread.h"

#define TLS_RETRIEVE_HELP_SPLIT_LIMIT   10

typedef enum {
    TLS_URGENT_ORDER = 0, 
    TLS_FAST_EMERGENCY_ORDER = 1, 
    TLS_EMERGENCY_ORDER = 2, 
    TLS_HIGH_ORDER = 3, 
    TLS_LOW_ORDER = 4, 
    TLS_NUMA_ORDER = 5, 
    TLS_DEFER_ORDER = 6, 
    TLS_DEFER_NUMA_ORDER = 7, 
    TLS_SPLIT_ORDER_NUM = 8
} TLS_SPLIT_ORDER;

typedef enum {
    TLS_URGENT_HELP_ORDER = 0, 
    //TLS_HIGH_HELP_ORDER = 1, 
    TLS_LOW_HELP_ORDER = 1, 
    TLS_NUMA_HELP_ORDER = 2, 
    TLS_HELP_ORDER_NUM = 3
} TLS_HELP_ORDER;

__thread struct tls_context *tls_context;
__thread struct tls_context *tls_context_array;
__thread struct per_node_context *per_node_context;
__thread int node_id;
__thread int tls_context_id;


struct node_context node_context;


static const SPLIT_MAP tls_split_map[TLS_SPLIT_ORDER_NUM] = {
                            FAST_URGENT_MAP, 
                            FAST_EMERGENCY_MAP, 
                            EMERGENCY_MAP, 
                            FAST_HIGH_MAP, 
                            FAST_LOW_MAP, 
                            FAST_NUMA_MAP, 
                            DEFER_MAP, 
                            DEFER_NUMA_MAP
};

static const HELP_MAP tls_help_map[TLS_HELP_ORDER_NUM] = {
                            HELP_URGENT_MAP, 
                            //HELP_HIGH_MAP, 
                            HELP_LOW_MAP, 
                            HELP_NUMA_MAP
};

static const HELP_MAP tls_split_map_help[TLS_SPLIT_ORDER_NUM] = {
                            HELP_URGENT_MAP, 
                            HELP_MAP_NUM, 
                            HELP_MAP_NUM, 
                            HELP_MAP_NUM,//HELP_HIGH_MAP, 
                            HELP_MAP_NUM, 
                            HELP_LOW_MAP, 
                            HELP_NUMA_MAP, 
                            HELP_MAP_NUM
};

static const SPLIT_MAP tls_help_map_split[TLS_SPLIT_ORDER_NUM] = {
                            URGENT_MAP, 
                            SPLIT_MAP_NUM, 
                            SPLIT_MAP_NUM, 
                            SPLIT_MAP_NUM,//HIGH_MAP, 
                            SPLIT_MAP_NUM, 
                            LOW_MAP, 
                            NUMA_MAP, 
                            SPLIT_MAP_NUM
};

void modify_tls_split_entry(
                    struct eh_segment *target_seg, 
                    struct eh_segment *dest_seg) {
    struct eh_split_context *s_context;
    struct eh_split_entry *s_ent, *new_ent;
    u64 hashed_prefix;
    int depth;
    
    s_context = tls_split_context();
    s_ent = &s_context->entry;

    new_ent = new_split_record(URGENT_PRIO, false);

    if (unlikely((void *)new_ent == INVALID_ADDR)) {
        //to dooooooooooo
        return;
    }

    hashed_prefix = s_context->hashed_prefix;
    depth = s_context->depth;

    if (unlikely(upgrade_eh_split_entry(s_ent, target_seg)))
        invalidate_eh_split_entry(new_ent);
    else
        init_eh_split_entry(new_ent, target_seg, dest_seg, 
                                hashed_prefix, depth, URGENT_SPLIT);
}

static int seize_tls_split_entry(struct eh_split_entry *split_ent) {
    struct eh_split_context *s_context;
    struct eh_segment *target_seg, *dest_seg;
    struct eh_split_entry ent, *tls_ent;
    EH_BUCKET_HEADER header, new_header, old_header;
    u64 hashed_prefix;
    int depth;
    SPLIT_TYPE type;
    
    ent.target = READ_ONCE(split_ent->target);

	if (unlikely(ent.target == INITIAL_EH_SPLIT_TARGET))
        return 1;

	if (unlikely(ent.target == INVALID_EH_SPLIT_TARGET))
		return -1;

	ent.destination = split_ent->destination;
	
	type = eh_split_type(&ent);
	depth = eh_split_depth(&ent);
	hashed_prefix = eh_split_prefix(&ent);

    if (type == INCOMPLETE_SPLIT)
        return 1;
	
	if (unlikely(!cas_bool(&split_ent->target, ent.target, INVALID_EH_SPLIT_TARGET)))
		return -1;
		
	target_seg = eh_split_target_seg(&ent);
	dest_seg = eh_split_dest_seg(&ent);
	
	s_context = tls_split_context();
	
	s_context->hashed_prefix = hashed_prefix;
	s_context->depth = depth;
	s_context->bucket_id = 0;
	s_context->dest_seg = dest_seg;
	s_context->inter_seg = NULL;
	s_context->type = type;
	
	tls_ent = tls_split_entry();
	
	if (type == NORMAL_SPLIT) {
        confirm_tls_split_target(target_seg, NORMAL_SPLIT);
        
        if (dest_seg == NULL) {
            dest_seg = boost_eh_defer_entry(tls_ent, target_seg);
            
            if (unlikely((void *)dest_seg == INVALID_ADDR)) {
                invalidate_eh_split_entry(tls_ent);
                return -1;
            }
            
            s_context->dest_seg = dest_seg;
            return 0;
        }
        
        header = READ_ONCE(dest_seg->bucket[0].header);
        new_header = set_eh_split_entry(tls_ent, THREAD_PRIO);
        
        while (likely(!eh_seg_low(header))) {
            old_header = cas(&dest_seg->bucket[0].header, header, new_header);
            
            if (likely(old_header == header))
                return 0;
                
            header = old_header;
        }
        
        dest_seg = eh_next_high_seg(header);
        goto seize_no_tls_split_entry;
    } else {
        inform_tls_split_target(target_seg);
        
        header = READ_ONCE(dest_seg->bucket[0].header);
        
        if (header != INITIAL_EH_BUCKET_TOP_HEADER)
            goto seize_no_tls_split_entry;
            
        header = READ_ONCE(dest_seg[2].bucket[0].header);
        
        if (header != INITIAL_EH_BUCKET_TOP_HEADER)
            goto seize_no_tls_split_entry;
            
        new_header = set_eh_split_entry(tls_ent, THREAD2_PRIO);
        
        if (!cas_bool(&dest_seg->bucket[0].header, INITIAL_EH_BUCKET_TOP_HEADER, new_header))
            goto seize_no_tls_split_entry;
                
        if (!cas_bool(&dest_seg[2].bucket[0].header, INITIAL_EH_BUCKET_TOP_HEADER, new_header)) {
            cas(&dest_seg->bucket[0].header, new_header, INITIAL_EH_BUCKET_TOP_HEADER);
            goto seize_no_tls_split_entry;
        }
        
        confirm_tls_split_target(target_seg, URGENT_SPLIT);
        
        memory_fence();
        header = READ_ONCE(dest_seg->bucket[0].header);
        
        if (unlikely(header != new_header)) {
            cas(&dest_seg[2].bucket[0].header, new_header, INITIAL_EH_BUCKET_TOP_HEADER);
            goto seize_no_tls_split_entry;
        }
        
        return 0;
    }

seize_no_tls_split_entry :
    modify_tls_split_entry(target_seg, dest_seg);
    return 0;
}

void retrieve_tls_help_split() {
    RECORD_POINTER tail, head, new_head, *split_tail, *split_head;
    struct eh_split_entry *ent, *tail_ent;
    struct record_page *h_page, *t_page;
    int h_ent_num, t_ent_num, limit, ret;
    TLS_SPLIT_ORDER order;
    SPLIT_MAP map;
    HELP_MAP h_map;
    bool again;

    for (order = TLS_URGENT_ORDER; order < TLS_SPLIT_ORDER_NUM; ++order) {
        map = tls_split_map[order];

        split_head = &tls_context->split_rp[map];
        split_tail = &tls_context->split_rp_tail[map];

        head = READ_ONCE(*split_head);
        acquire_fence();

        tail = READ_ONCE(*split_tail);
        
        again = true;
        
        if (tail != head && 
                ((order != TLS_DEFER_NUMA_ORDER && order != TLS_DEFER_ORDER) || is_prefault_seg_enough(2)) && 
                ((order != TLS_FAST_EMERGENCY_ORDER && order != TLS_EMERGENCY_ORDER) || is_prefault_seg_enough(4))) {
            t_page = page_of_record_pointer(tail);
            t_ent_num = ent_num_of_record_pointer(tail);

            h_page = page_of_record_pointer(head);
            h_ent_num = ent_num_of_record_pointer(head);

        retrieve_tls_help_split_again :
            tail_ent = &t_page->s_ent[t_ent_num];
            ent = &h_page->s_ent[h_ent_num];

            limit = 0;

            while (ent != tail_ent) {
                ret = seize_tls_split_entry(ent);

                if (unlikely(ret == 1))
                    break;

                if (h_ent_num == SPLIT_ENT_PER_RECORD_PAGE - 1) {
                    new_head = make_record_pointer(h_page->next, 0);

                    if (cas_bool(split_head, head, new_head))
                        reclaim_concurrent_record_page(h_page);

                    return;
                }

                h_ent_num += 1;

                ent = &h_page->s_ent[h_ent_num];

                if (ret == -1 && ++limit <= TLS_RETRIEVE_HELP_SPLIT_LIMIT)
                    continue;

                new_head = make_record_pointer(h_page, h_ent_num);
                cas(split_head, head, new_head);

                if (ret == 0)
                    return;

                break;
            }
        }
        
        h_map = tls_split_map_help[order];

        if (h_map < HELP_MAP_NUM && again) {
            map = tls_help_map_split[order];

            split_head = &tls_context->split_rp[map];
            split_tail = &tls_context->help_rp[h_map];

            head = READ_ONCE(*split_head);
            acquire_fence();

            tail = READ_ONCE(*split_tail);
                
            if (tail != INVALID_RECORD_POINTER) {
                t_page = page_of_record_pointer(tail);
                t_ent_num = ent_num_of_record_pointer(tail);

                h_page = page_of_record_pointer(head);
                h_ent_num = ent_num_of_record_pointer(head);

                if (h_page == t_page && h_ent_num >= t_ent_num)
                    WRITE_ONCE(*split_tail, INVALID_RECORD_POINTER);
                else {
                    again = false;
                    goto retrieve_tls_help_split_again;
                }
            }
        }
    }
}

void release_all_tls_context() {
    //to dooooooooooooooooooooooooo
}

int prepare_all_tls_context() {
    struct per_node_context *per_nc;
    struct tls_context *tls;
    struct record_page *r_page;
    void *r_pool_base;
    void *seg_backup;
    size_t r_size;
    int i, j, k, n, mt, count, total_threads, id, dis;

    n = node_context.node_num;
    per_nc = node_context.all_node_context;

    for (i = 0; i < n; ++i) {
        total_threads = per_nc[i].total_thread_num;

        if (total_threads == 0)
            continue;

        for (j = 0; j < total_threads; ++j) {
            tls = &per_nc[i].max_tls_context[j];

            for (k = 0; k < SPLIT_MAP_NUM; ++k)
                tls->split_rp[k] = INVALID_RECORD_POINTER;

            //tls->chunk_reclaim = INVALID_RECORD_POINTER;
            tls->page_reclaim = INVALID_RECORD_POINTER;

            tls->seg_pool.pool = 0;
            tls->record_pool.pool = 0;
            
            tls->numa_seg_pool.pool = 0;
            tls->numa_extra_seg.pool = 0;
            tls->numa_record_pool.pool = 0;
        }
    }

    for (i = 0; i < n; ++i) {
        count = 0;

        total_threads = per_nc[i].total_thread_num;

        if (total_threads == 0)
            continue;

        mt = per_nc[i].max_work_thread_num;

        r_size = RECORD_PAGE_SIZE * (SPLIT_MAP_NUM + GC_MAP_NUM + 1) * total_threads;//modifyyyyyyyyyyyyyyyyyyyyyyy
        r_page = (struct record_page *)alloc_node_page(r_size, i);

        if (unlikely(r_page == (struct record_page *)INVALID_ADDR)) {
            release_all_tls_context();
            return -1;
        }

        //memset(r_page, 0, r_size);

        for (j = 0; j < total_threads; ++j) {
            tls = &per_nc[i].max_tls_context[j];

            tls->epoch = MAX_LONG_INTEGER;

            tls->seg_prefault_hint = tls->seg_prefault_count = 0;

            tls->seg_free_self = tls->seg_free = INVALID_RECORD_POINTER;
            
            for (k = 0; k < SPLIT_MAP_NUM; ++k) {
                r_page[count].counter = 0;
                r_page[count].next = NULL;
                tls->split_rp[k] = make_record_pointer(&r_page[count++], 0);
                tls->split_rp_tail[k] = tls->split_rp[k];
            }

            for (k = 0; k < GC_MAP_NUM; ++k) {
                r_page[count].next = NULL;
                r_page[count].counter = 0;
                tls->gc_rp[k] = make_record_pointer(&r_page[count++], 0);
                tls->gc_rp_tail[k] = tls->gc_rp[k];
            }
            
            for (k = 0; k < HELP_MAP_NUM; ++k)
                tls->help_rp[k] = INVALID_RECORD_POINTER;

            /*r_page[count].carve = (void *)&(r_page[count].r_ent[0]);
            tls->chunk_reclaim = make_record_pointer(&r_page[count++], 0);
            tls->chunk_reclaim_tail = tls->chunk_reclaim;*/

            r_page[count].carve = (void *)&(r_page[count].r_ent[0]);
            tls->page_reclaim = make_record_pointer(&r_page[count++], 0);
            tls->page_reclaim_tail = tls->page_reclaim;

            tls->seg_prefault = alloc_node_aligned_page(EH_ALLOC_POOL_SIZE, i, EH_SEGMENT_SIZE_BITS);

            if (unlikely(tls->seg_prefault == INVALID_ADDR))
                goto prepare_all_tls_context_failed;

            tls->seg_pool.page_num = 0;
            tls->seg_pool.base_page = DIV_2((uintptr_t)tls->seg_prefault, PAGE_SHIFT);

            seg_backup = alloc_node_aligned_page(EH_ALLOC_POOL_SIZE, i, EH_SEGMENT_SIZE_BITS);

            if (unlikely(seg_backup == INVALID_ADDR))
                goto prepare_all_tls_context_failed;

            tls->seg_backup.page_num = 0;
            tls->seg_backup.base_page = DIV_2((uintptr_t)seg_backup, PAGE_SHIFT);

            r_pool_base = alloc_node_page(RECORD_POOL_SIZE, i);

            if (unlikely(r_pool_base == INVALID_ADDR))
                goto prepare_all_tls_context_failed;
            
            tls->record_pool.page_num = 0;
            tls->record_pool.base_page = DIV_2((uintptr_t)r_pool_base, PAGE_SHIFT);

            tls->thread_split_context.state = FEW_SPLITS;

            tls->chunk_alloc_hot = tls->chunk_alloc = NULL;

            if (j < mt) {
                tls->thread_split_context.thread = true;
                tls->thread_split_context.entry.target = INVALID_EH_SPLIT_TARGET;
                tls->numa_seg_pool.other_page_num = MAX_EH_SEG_FOR_OTHER_POOL;

                r_pool_base = alloc_node_page(RECORD_POOL_SIZE, i);

                if (unlikely(r_pool_base == INVALID_ADDR))
                    goto prepare_all_tls_context_failed;
            
                tls->numa_record_pool.page_num = 0;
                tls->numa_record_pool.base_page = DIV_2((uintptr_t)r_pool_base, PAGE_SHIFT);

                //tls->splited = 0;
                id = (j / THREADS_PER_SPLIT_THREAD) * THREADS_PER_SPLIT_THREAD;
                dis = (id + THREADS_PER_SPLIT_THREAD > mt ? mt - id : THREADS_PER_SPLIT_THREAD);
                
                tls->tid_begin = j - DIV_2(dis, 1);
                tls->tid_end = j;
        
                if (tls->tid_begin < id)
                    tls->tid_begin += dis;
            } else {
                /*tls->consume_rate = 0.0;
                tls->splits_statis = 0;
                tls->splited_statis = 0;*/
                
                tls->tid_begin = (j - mt) * THREADS_PER_SPLIT_THREAD;
                tls->tid_end = tls->tid_begin + THREADS_PER_SPLIT_THREAD;
                tls->rr_tid = tls->tid_begin;
        
                if (tls->tid_end > mt)
                    tls->tid_end = mt;
            
                tls->tls_begin = &per_nc[i].max_tls_context[0];
            }

            continue;
        
        prepare_all_tls_context_failed :
            free_node_page(&r_page[count], 
                                r_size - count * RECORD_PAGE_SIZE * 4);
            release_all_tls_context();
            return -1;
        }
    }

    memory_fence();
    return 0;
}

void init_tls_context(int nid, int tid) {
    unsigned long epoch;
    int id, work_threads;

    node_id = nid;
    tls_context_id = tid;

    per_node_context = &node_context.all_node_context[nid];
    tls_context_array = per_node_context->max_tls_context;
    tls_context = tls_context_of_tid(tid);
    
    work_threads = per_node_context->max_work_thread_num;

    if (tid >= per_node_context->max_work_thread_num) {
        //tls_context->sample_time = sys_time_us();
        /*tls_context->tid_begin = (tid - work_threads) * THREADS_PER_SPLIT_THREAD;
        tls_context->tid_end = tls_context->tid_begin + THREADS_PER_SPLIT_THREAD;
        tls_context->rr_tid = tls_context->tid_begin;
        
        if (tls_context->tid_end > work_threads)
            tls_context->tid_end = work_threads;
            
        tls_context->tls_begin = &tls_context_array[tls_context->tid_begin];*/

        epoch = 0;
    } else {
        id = (tid / THREADS_PER_SPLIT_THREAD) + work_threads;
        tls_context->background_tls = tls_context_of_tid(id);
        epoch = READ_ONCE(tls_context->background_tls->epoch);

        if (epoch == MAX_LONG_INTEGER)
            epoch = 0;
            
        /*id = (tid / THREADS_PER_SPLIT_THREAD) * THREADS_PER_SPLIT_THREAD;
        dis = (id + THREADS_PER_SPLIT_THREAD > work_threads ? work_threads - id : THREADS_PER_SPLIT_THREAD);
            
        tls_context->tid_begin = tid - DIV_2(dis, 1);
        tls_context->tid_end = tid - 1;
        
        if (tls_context->tid_begin < id)
            tls_context->tid_begin += dis;
            
        if (tls_context->tid_end < id)
            tls_context->tid_end += dis;*/
    }

    release_fence();
    WRITE_ONCE(tls_context->epoch, epoch);

    memory_fence();
}


struct eh_split_entry *new_split_record(
                            SPLIT_PRIORITY prio, 
                            bool fast) {
    RECORD_POINTER new_pointer, *record_ptr;
    struct record_page *page, *new_page;
    int ent_num;

    switch (prio) {
    case LOW_PRIO:
        record_ptr = fast ? &tls_context->split_rp_tail[FAST_LOW_MAP] : 
                    &tls_context->split_rp_tail[LOW_MAP];
        break;
    case DEFER_PRIO:
        record_ptr = &tls_context->split_rp_tail[DEFER_MAP];
        break;
    case THREAD_PRIO:
        return tls_split_entry();
    case HIGH_PRIO:
        record_ptr = fast ? &tls_context->split_rp_tail[FAST_HIGH_MAP] : 
                    &tls_context->split_rp_tail[HIGH_MAP];
        break;
    case URGENT_PRIO:
        record_ptr = fast ? &tls_context->split_rp_tail[FAST_URGENT_MAP] : 
                    &tls_context->split_rp_tail[URGENT_MAP];
        break;
    case EMERGENCY_PRIO:
        record_ptr = fast ? &tls_context->split_rp_tail[FAST_EMERGENCY_MAP] : 
                    &tls_context->split_rp_tail[EMERGENCY_MAP];
        break;
    case INCOMPLETE_PRIO:
        record_ptr = &tls_context->split_rp_tail[INCOMPLETE_MAP];
        break;
    default:
        return NULL;
    }

    page = page_of_record_pointer(*record_ptr);
    ent_num = ent_num_of_record_pointer(*record_ptr);

    if (likely(ent_num != SPLIT_ENT_PER_RECORD_PAGE - 1))
        new_pointer = make_record_pointer(page, ent_num + 1);
    else {
        new_page = alloc_record_page();

        if (unlikely((void *)new_page == INVALID_ADDR))
            return (struct eh_split_entry *)INVALID_ADDR;

        page->next = new_page;

        new_page->next = NULL;
        new_page->counter = page->counter + SPLIT_ENT_PER_RECORD_PAGE;

        new_pointer = make_record_pointer(new_page, 0);
    }

    release_fence();
    WRITE_ONCE(*record_ptr, new_pointer);

    return &page->s_ent[ent_num];
}

struct reclaim_entry *new_gc_record(GC_PRIORITY prio) {
    RECORD_POINTER new_pointer, *record_ptr;
    struct record_page *page, *new_page;
    int ent_num;

    switch (prio) {
    case GC_SPARSE_PRIO:
        record_ptr = &tls_context->gc_rp_tail[GC_SPARSE_MAP];
        break;
    case GC_DENSE_PRIO:
        record_ptr = &tls_context->gc_rp_tail[GC_DENSE_MAP];
        break;
    case GC_LOW_PRIO:
        record_ptr = &tls_context->gc_rp_tail[GC_LOW_MAP];
        break;
    default:
        return NULL;
    }

    page = page_of_record_pointer(*record_ptr);
    ent_num = ent_num_of_record_pointer(*record_ptr);

    if (likely(ent_num != RECLAIM_ENT_PER_RECORD_PAGE - 1))
        new_pointer = make_record_pointer(page, ent_num + 1);
    else {
        new_page = alloc_record_page();

        if (unlikely((void *)new_page == INVALID_ADDR))
            return (struct reclaim_entry *)INVALID_ADDR;

        page->next = new_page;
        new_page->next = NULL;
        new_page->counter = 0;

        new_pointer = make_record_pointer(new_page, 0);
    }

    release_fence();
    WRITE_ONCE(*record_ptr, new_pointer);

    return &page->r_ent[ent_num];
}

struct eh_split_entry *new_other_split_record(
                            SPLIT_PRIORITY prio, 
                            int other_nid, 
                            bool fast) {
    RECORD_POINTER rp, new_rp, *record_ptr;
    struct per_node_context *per_nc;
    struct tls_context *tls;
    struct record_page *page, *new_page;
    struct eh_split_entry *ret_ent;
    int ent_num, tid;

    per_nc = &node_context.all_node_context[other_nid];
    tid = tid_itself() % per_nc->max_work_thread_num;
    tls = &per_nc->max_tls_context[tid];

    if (prio == URGENT_PRIO)
        record_ptr = &tls->split_rp_tail[URGENT_NUMA_MAP];
    else if (prio == DEFER_PRIO)
        record_ptr = &tls_context->split_rp_tail[DEFER_NUMA_MAP];
    else
        record_ptr = fast ? &tls->split_rp_tail[FAST_NUMA_MAP] : 
                                &tls->split_rp_tail[NUMA_MAP];

new_other_split_record_again :
    rp = READ_ONCE(*record_ptr);

    page = page_of_record_pointer(rp);
    ent_num = ent_num_of_record_pointer(rp);

    ret_ent = &page->s_ent[ent_num];

    if (likely(ent_num != SPLIT_ENT_PER_RECORD_PAGE - 1)) {
        new_rp = make_record_pointer(page, ent_num + 1);

        if (unlikely(!cas_bool(record_ptr, rp, new_rp)))
            goto new_other_split_record_again;
    } else {
        new_page = READ_ONCE(page->next);

        if (new_page) {
            new_rp = make_record_pointer(new_page, 0);
            cas(record_ptr, rp, new_rp);

            goto new_other_split_record_again;
        }

        new_page = alloc_other_record_page(other_nid);

        if (unlikely((void *)new_page == INVALID_ADDR))
            return (struct eh_split_entry *)INVALID_ADDR;

        new_page->next = NULL;
        new_page->counter = page->counter + SPLIT_ENT_PER_RECORD_PAGE;

        page = cas(&page->next, NULL, new_page);

        if (unlikely(page != NULL)) {
            free_node_page(new_page, RECORD_PAGE_SIZE);

            new_rp = make_record_pointer(page, 0);
            cas(record_ptr, rp, new_rp);

            goto new_other_split_record_again;
        }

        new_rp = make_record_pointer(new_page, 0);
        cas(record_ptr, rp, new_rp);
    }

    return ret_ent;
}

struct eh_split_entry *new_per_split_record(
                            SPLIT_PRIORITY prio, 
                            bool fast) {
    struct eh_split_entry *ent;
    int tid, tid_begin, tid_end, t;
    
    tid_begin = tls_context->tid_begin;
    tid_end = tls_context->tid_end;
    
    tid = tls_context->rr_tid;

    tls_context->rr_tid = (tid == tid_end) ? tid_begin : tid + 1;
    
    t = tls_context_id;
    tls_context_id = tid;
    
    ent = new_other_split_record(prio, node_id, fast);
    
    tls_context_id = t;
    
    return ent;
}

/*int reclaim_chunk(void *addr) {
    RECORD_POINTER r, record_ptr;
    struct record_page *page, *new_page;
    int ent_num;

    record_ptr = tls_context->chunk_reclaim_tail;
    page = page_of_record_pointer(record_ptr);
    ent_num = ent_num_of_record_pointer(record_ptr);

    if (likely(ent_num != RECLAIM_ENT_PER_RECORD_PAGE - 1))
        r = make_record_pointer(page, ent_num + 1);
    else {
        new_page = alloc_record_page();

        if (unlikely((void *)new_page == INVALID_ADDR))
            return -1;

        page->next = new_page;
        r = make_record_pointer(new_page, 0);
    }

    page->r_ent[ent_num].addr = (uintptr_t)addr;

    release_fence();
    WRITE_ONCE(tls_context->chunk_reclaim_tail, r);

    return 0;
}*/

int reclaim_page(void *addr, int shift) {
    RECORD_POINTER r, record_ptr;
    struct record_page *page, *new_page;
    int ent_num;

    record_ptr = tls_context->page_reclaim_tail;
    page = page_of_record_pointer(record_ptr);
    ent_num = ent_num_of_record_pointer(record_ptr);

    if (likely(ent_num != RECLAIM_ENT_PER_RECORD_PAGE - 1))
        r = make_record_pointer(page, ent_num + 1);
    else {
        new_page = alloc_record_page();

        if (unlikely((void *)new_page == INVALID_ADDR))
            return -1;

        page->next = new_page;
        r = make_record_pointer(new_page, 0);
    }

    page->r_ent[ent_num].addr = make_page_reclaim_ent(addr, shift);

    release_fence();
    WRITE_ONCE(tls_context->page_reclaim_tail, r);

    return 0;
}
