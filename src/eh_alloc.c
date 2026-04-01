#include "eh_alloc.h"

#include "per_thread.h"

void hint_eh_seg_prefault(int num) {
    struct tls_context *tls;
    unsigned long hints;

    tls = tls_context_itself();

    hints = tls->seg_prefault_hint;

    release_fence();
    WRITE_ONCE(tls->seg_prefault_hint, hints + num);
}

bool is_prefault_seg_enough(int num) {
    struct tls_context *tls;
    PAGE_POOL seg_pool;
    u64 seg_base, seg_num;
    void *addr, *prefault;

    tls = tls_context_itself();

    seg_pool.pool = READ_ONCE(tls->seg_pool.pool);
    acquire_fence();

    prefault = READ_ONCE(tls->seg_prefault);

    seg_base = seg_pool.base_page;
    seg_num = num + seg_pool.page_num;

    addr = (void *)MUL_2(seg_base, PAGE_SHIFT) + MUL_2(seg_num, EH_SEGMENT_SIZE_BITS);

    return (prefault >= addr ? true : false);
}

static struct eh_segment *steal_eh_seg(
            struct tls_context *tls, 
            int num) {
    struct tls_context *background_tls, *tls_begin, *stolen;
    PAGE_POOL seg_pool, pool;
    u64 seg_base, seg_num;
    struct eh_segment *seg;
    void *prefault;
    int tid_begin, tid_end, tid_middle;
    bool last;
    
    background_tls = tid_itself() < per_node_context->max_work_thread_num ? 
                        tls->background_tls : tls_context_itself();
                        
    tls_begin = background_tls->tls_begin;
    
    tid_begin = tls->tid_begin;
    tid_end = tls->tid_end;
    tid_middle = (tid_begin > tid_end ? background_tls->tid_end : tid_end);
    
    last = false;
    
    while (1) {
        if (tid_begin < tid_middle)
            stolen = &tls_begin[tid_begin];
        else if (tid_begin == tid_end) {
            stolen = background_tls;
            last = true;
        } else if (tid_begin == tid_middle) {
            tid_begin = background_tls->tid_begin;
            tid_middle = tid_end;
            continue;
        }
        
        seg_pool.pool = READ_ONCE(stolen->seg_pool.pool);
        acquire_fence();
        
        prefault = READ_ONCE(stolen->seg_prefault);

        seg_base = seg_pool.base_page;
        seg_num = seg_pool.page_num;

        seg = (struct eh_segment *)MUL_2(seg_base, PAGE_SHIFT);
        
        if ((void *)&seg[num + seg_num] <= prefault) {
            pool.base_page = seg_base;
            pool.page_num = seg_num + num;

            pool.pool = cas(&stolen->seg_pool.pool, seg_pool.pool, pool.pool);

            if (likely(pool.pool == seg_pool.pool))
                return &seg[seg_num];
        }
        
        if (last)
            break;
        
        tid_begin += 1;
    }
    
    return NULL;
}

struct eh_segment *__alloc_eh_seg(
                        struct tls_context *tls, 
                        int num, int nid, 
                        bool *already_paged) {
    PAGE_POOL seg_pool, pool;
    u64 seg_base, seg_num, size;
    struct eh_segment *seg, *stolen;
    void *prefault, *addr;

    seg_pool.pool = READ_ONCE(tls->seg_pool.pool);
    acquire_fence();

re_alloc_eh_seg :
    prefault = READ_ONCE(tls->seg_prefault);

    seg_base = seg_pool.base_page;
    seg_num = seg_pool.page_num;

    seg = (struct eh_segment *)MUL_2(seg_base, PAGE_SHIFT);

    if ((void *)&seg[num + seg_num] <= prefault) {
        pool.base_page = seg_base;
        pool.page_num = seg_num + num;

        pool.pool = cas(&tls->seg_pool.pool, seg_pool.pool, pool.pool);

        if (likely(pool.pool == seg_pool.pool)) {
            *already_paged = true;
            return &seg[seg_num];
        }

        seg_pool.pool = pool.pool;
        goto re_alloc_eh_seg;
    }
    
    stolen = steal_eh_seg(tls, num);
    
    if (stolen) {
        *already_paged = true;
        return stolen;
    }

    *already_paged = false;

    if (unlikely(prefault == (void *)&seg[MAX_EH_SEG_IN_POOL]) && 
                            cas_bool(&tls->seg_prefault, prefault, NULL))
        prefault = NULL;

    if (unlikely(prefault == NULL)) {
        addr = alloc_node_aligned_page(EH_ALLOC_POOL_SIZE, nid, EH_SEGMENT_SIZE_BITS);

        if (addr != INVALID_ADDR) {
            pool.base_page = DIV_2((uintptr_t)addr, PAGE_SHIFT);
            pool.page_num = 0;
            
            if (cas_bool(&tls->seg_pool.pool, seg_pool.pool, pool.pool)) {
                WRITE_ONCE(tls->seg_prefault, addr);

                size = MUL_2(MAX_EH_SEG_IN_POOL - seg_num, EH_SEGMENT_SIZE_BITS);
                free_page_aligned((void *)&seg[seg_num], size);
            } else
                free_page_aligned(addr, EH_ALLOC_POOL_SIZE);
        }
    }

    seg_pool.pool = READ_ONCE(tls->seg_backup.pool);

re_alloc_eh_backup_seg :
    seg_base = seg_pool.base_page;
    seg_num = seg_pool.page_num;

    seg = (struct eh_segment *)MUL_2(seg_base, PAGE_SHIFT);

    if (likely(seg_num + num <= MAX_EH_SEG_IN_POOL)) {
        pool.base_page = seg_base;
        pool.page_num = seg_num + num;

        pool.pool = cas(&tls->seg_backup.pool, seg_pool.pool, pool.pool);

        if (likely(pool.pool == seg_pool.pool))
            return &seg[seg_num];

        seg_pool.pool = pool.pool;
        goto re_alloc_eh_backup_seg;
    }

    if (seg_num != MAX_EH_SEG_IN_POOL) {
        pool.base_page = seg_base;
        pool.page_num = MAX_EH_SEG_IN_POOL;

        if (!cas_bool(&tls->seg_backup.pool, seg_pool.pool, pool.pool)) {
            size = MUL_2(num, EH_SEGMENT_SIZE_BITS);
            return (struct eh_segment *)alloc_node_aligned_page(size, nid, EH_SEGMENT_SIZE_BITS);
        }

        size = MUL_2(MAX_EH_SEG_IN_POOL - seg_num, EH_SEGMENT_SIZE_BITS);
        free_page_aligned((void *)&seg[seg_num], size);

        seg_pool.pool = pool.pool;
    }

    addr = alloc_node_aligned_page(EH_ALLOC_POOL_SIZE, nid, EH_SEGMENT_SIZE_BITS);

    if (addr != INVALID_ADDR) {
        pool.base_page = DIV_2((uintptr_t)addr, PAGE_SHIFT);
        pool.page_num = num;
            
        if (cas_bool(&tls->seg_backup.pool, seg_pool.pool, pool.pool))
            return (struct eh_segment *)addr;
        
        free_page_aligned(addr, EH_ALLOC_POOL_SIZE);
    }

    size = MUL_2(num, EH_SEGMENT_SIZE_BITS);
    return (struct eh_segment *)alloc_node_aligned_page(size, nid, EH_SEGMENT_SIZE_BITS);
}

struct eh_segment *alloc_eh_seg(
                    int num,
                    bool *already_paged) {
    struct tls_context *tls;
    int nid;

    tls = tls_context_itself();
    nid = tls_node_id();

    return __alloc_eh_seg(tls, num, nid, already_paged);
}


struct eh_segment *alloc_other_eh_seg(
                            int num, int nid, 
                            bool *already_paged) {
    struct per_node_context *per_nc;
    struct tls_context *tls;
    PAGE_POOL other_seg_pool, other_pool, extra_pool;
    u64 p, seg_base, seg_num, size;
    struct eh_segment *seg;
    int tid;
    bool paged;

    per_nc = &node_context.all_node_context[nid];
    tid = tid_itself() % per_nc->max_work_thread_num;
    tls = &per_nc->max_tls_context[tid];

    extra_pool.pool = READ_ONCE(tls->numa_extra_seg.pool);

    seg_base = extra_pool.other_base_page;
    seg_num = extra_pool.other_page_num;
    paged = (bool)extra_pool.other_paged;

    if (seg_num == num && cas_bool(
                &tls->numa_extra_seg.pool, extra_pool.pool, 0)) {
        seg = (struct eh_segment *)MUL_2(seg_base, PAGE_SHIFT);
        *already_paged = paged;
        return (void *)seg;
    }

    other_seg_pool.pool = READ_ONCE(tls->numa_seg_pool.pool);

re_alloc_other_eh_seg :
    seg_base = other_seg_pool.other_base_page;
    seg_num = other_seg_pool.other_page_num;
    paged = (bool)other_seg_pool.other_paged;

    seg = (struct eh_segment *)MUL_2(seg_base, PAGE_SHIFT);

    if (likely(seg_num + num <= MAX_EH_SEG_FOR_OTHER_POOL)) {
        other_pool.other_base_page = seg_base;
        other_pool.other_page_num = seg_num + num;
        other_pool.other_paged = paged;

        p = cas(&tls->numa_seg_pool.pool, other_seg_pool.pool, other_pool.pool);

        if (likely(p == other_seg_pool.pool)) {
            *already_paged = paged;
            return &seg[seg_num];
        }

        other_seg_pool.pool = p;
        goto re_alloc_other_eh_seg;
    }

    if (seg_num != MAX_EH_SEG_FOR_OTHER_POOL) {
        other_pool.other_base_page = seg_base;
        other_pool.other_page_num = MAX_EH_SEG_FOR_OTHER_POOL;
        other_pool.other_paged = paged;

        p = cas(&tls->numa_seg_pool.pool, other_seg_pool.pool, other_pool.pool);

        if (unlikely(p != other_seg_pool.pool)) {
            other_seg_pool.pool = p;
            goto re_alloc_other_eh_seg;
        }

        extra_pool.other_base_page = DIV_2((uintptr_t)&seg[seg_num], PAGE_SHIFT);
        extra_pool.other_page_num = MAX_EH_SEG_FOR_OTHER_POOL - seg_num;
        extra_pool.other_paged = paged;

        if (!cas_bool(&tls->numa_extra_seg.pool, 0, extra_pool.pool)) {
            size = MUL_2(extra_pool.page_num, EH_SEGMENT_SIZE_BITS);
            free_page_aligned((void *)&seg[seg_num], size);
        }

        other_seg_pool.pool = other_pool.pool;
    }

    seg = __alloc_eh_seg(tls, MAX_EH_SEG_FOR_OTHER_POOL, nid, already_paged);

    if (seg != INVALID_ADDR) {
        other_pool.other_base_page = DIV_2((uintptr_t)seg, PAGE_SHIFT);
        other_pool.other_page_num = num;
        other_pool.other_paged = *already_paged;

        p = cas(&tls->numa_seg_pool.pool, other_seg_pool.pool, other_pool.pool);

        if (unlikely(p != other_seg_pool.pool)) {
            free_page_aligned((void *)seg, EH_OTHER_ALLOC_POOL_SIZE);

            other_seg_pool.pool = p;
            goto re_alloc_other_eh_seg;
        }
    }

    return seg;
}


struct record_page *alloc_record_page() {
    struct tls_context *tls;
    u64 pool_page_base, pool_page_num;
    struct record_page *r_page;

    tls = tls_context_itself();
    
    pool_page_base = tls->record_pool.base_page;
    pool_page_num = tls->record_pool.page_num;

    if (unlikely(pool_page_num == MAX_RECOED_PAGE_IN_POOL)) {
        r_page = (struct record_page *)alloc_node_page(
                                    RECORD_POOL_SIZE, tls_node_id());

        if (unlikely((void *)r_page == INVALID_ADDR))
            return (struct record_page *)malloc_page_aligned(
                                                    RECORD_PAGE_SIZE);

        tls->record_pool.page_num = 1;
        tls->record_pool.base_page = DIV_2((uintptr_t)r_page, PAGE_SHIFT);

        return r_page;
    }

    tls->record_pool.page_num = pool_page_num + 1;

    r_page = (struct record_page *)MUL_2(pool_page_base, PAGE_SHIFT);
    r_page += pool_page_num;

    return r_page;
}

struct record_page *alloc_other_record_page(int nid) {
    struct per_node_context *per_nc;
    struct tls_context *tls;
    u64 pool_page_base, pool_page_num, pool;
    struct record_page *r_page;
    PAGE_POOL rec_pool, new_pool;
    int tid;

    per_nc = &node_context.all_node_context[nid];
    tid = tid_itself() % per_nc->max_work_thread_num;
    tls = &per_nc->max_tls_context[tid];

    rec_pool.pool = READ_ONCE(tls->numa_record_pool.pool);

re_alloc_other_record_page :
    pool_page_base = rec_pool.base_page;
    pool_page_num = rec_pool.page_num;

    if (unlikely(pool_page_num == MAX_RECOED_PAGE_IN_POOL)) {
        r_page = (struct record_page *)alloc_node_page(
                                        RECORD_POOL_SIZE, nid);

        if (unlikely((void *)r_page == INVALID_ADDR))
            return (struct record_page *)malloc_page_aligned(
                                                    RECORD_PAGE_SIZE);

        new_pool.base_page = DIV_2((uintptr_t)r_page, PAGE_SHIFT);
        new_pool.page_num = 1;

        pool = cas(&tls->numa_record_pool.pool, rec_pool.pool, new_pool.pool);

        if (unlikely(rec_pool.pool != pool)) {
            rec_pool.pool = pool;
            goto re_alloc_other_record_page;
        }

        return r_page;
    }

    new_pool.base_page = pool_page_base;
    new_pool.page_num = pool_page_num + 1;

    pool = cas(&tls->numa_record_pool.pool, rec_pool.pool, new_pool.pool);

    if (unlikely(rec_pool.pool != pool)) {
        rec_pool.pool = pool;
        goto re_alloc_other_record_page;
    }

    r_page = (struct record_page *)MUL_2(pool_page_base, PAGE_SHIFT);
    r_page += pool_page_num;

    return r_page;
}

int prefault_eh_seg(int tid) {
    struct tls_context *tls;
    PAGE_POOL seg_pool;
    void *addr, *addr_end, *pre_addr, *pre_addr2;
    unsigned long seg_base, seg_num, all_hints, hints, faults, fault_size;
    int defer_num, ret;

    tls = tls_context_of_tid(tid);

    seg_pool.pool = READ_ONCE(tls->seg_pool.pool);
    acquire_fence();

    seg_base = seg_pool.base_page;
    seg_num = seg_pool.page_num;

    addr = (void *)MUL_2(seg_base, PAGE_SHIFT);
    addr_end = addr + EH_ALLOC_POOL_SIZE;

    pre_addr = READ_ONCE(tls->seg_prefault);

    if (unlikely(pre_addr >= addr_end) || unlikely(pre_addr < addr))
        return 0;

    ret = 0;

    addr += MUL_2(seg_num, EH_SEGMENT_SIZE_BITS);

    all_hints = READ_ONCE(tls->seg_prefault_hint);
    acquire_fence();

    hints = all_hints - tls->seg_prefault_count;
    
    defer_num = tls_split_record_num(tls, DEFER_NUMA_MAP) + tls_split_record_num(tls, DEFER_MAP);

    if (hints > MAX_EH_PREFETCH_SEG || 
            SHIFT_OF(defer_num, EH_SEGMENT_SIZE_BITS + 1) > pre_addr - addr) {
        ret = 1;
        faults = MAX_EH_PREFETCH_SEG;
    } else
        faults = hints;

    fault_size = MUL_2(faults, EH_SEGMENT_SIZE_BITS);

    if (addr + EH_PREFETCH_SEG_SIZE > pre_addr + fault_size)
        fault_size = EH_PREFETCH_SEG_SIZE;//fault_size = (addr + EH_PREFETCH_SEG_SIZE) - pre_addr;

    /*if (addr + EH_PREFETCH_SEG_SIZE < pre_addr)
        tls->seg_prefault_count += faults;
    else
        tls->seg_prefault_count = all_hints;*/
    tls->seg_prefault_count = all_hints;

    pre_addr2 = pre_addr + fault_size;

    if (unlikely(pre_addr2 >= addr_end)) {
        if (prefault_page_aligned(pre_addr, addr_end - pre_addr) != -1)
            WRITE_ONCE(tls->seg_prefault, addr_end);
    } else if (fault_size != 0 && 
            likely(prefault_page_aligned(pre_addr, fault_size) != -1))
        WRITE_ONCE(tls->seg_prefault, pre_addr2);

    return ret;
}
