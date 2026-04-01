#include "eh_chunk.h"
#include "eh_alloc.h"
#include "eh_dir.h"
#include "per_thread.h"
#include "kv.h"


void dealloc_eh_chunk(void *addr, int size) {
    struct eh_chunk_metadata *chunk_meta;
    void *chunk;
    BITMAP_ENT *bitmap_head, *bitmap_ent, bitmap;
    struct reclaim_entry *r_ent, *new_r_ent;
    unsigned long diff;
    u64 time, latest_time;
    int ents, bits;
    unsigned invalid, new_invalid;
    GC_PRIORITY prio, new_prio;

    chunk = (void *)(EH_CHUNK_MASK & (uintptr_t)addr);

    bitmap_head = (BITMAP_ENT *)(chunk + EH_CHUNK_BITMAP_OFFSET);

    chunk_meta = (struct eh_chunk_metadata *)(chunk + EH_CHUNK_METADATA_OFFSET);

    diff = (~EH_CHUNK_MASK) & (uintptr_t)addr;

    ents = DIV_2(diff, EH_BITS_PER_CHUNK_BITMAP_ENT);
    bitmap_ent = &bitmap_head[ents];

    diff &= ~MASK(EH_BITS_PER_CHUNK_BITMAP_ENT);

    bits = DIV_2(diff, EH_CHUNK_ALLOC_UNIT_BITS);

    while (1) {
        bitmap = READ_ONCE(*bitmap_ent);

        if (!(bitmap & EXP_2(bits)))
            return;

        if (likely(cas_bool(bitmap_ent, bitmap, bitmap & ~EXP_2(bits))))
            break;
    }

    while (1) {
        invalid = READ_ONCE(chunk_meta->invalid_bytes);
        new_invalid = invalid + size;

        if (cas_bool(&chunk_meta->invalid_bytes, invalid, new_invalid))
            break;
    }

    if (unlikely(new_invalid == EH_CHUNK_RAW_DATA_SIZE)) {
        prio = READ_ONCE(chunk_meta->priority);

        if (prio != GC_IDLE_PRIO && prio != GC_NO_PRIO) {
            r_ent = READ_ONCE(chunk_meta->r_entry);

            if (likely(r_ent != NULL))
                WRITE_ONCE(r_ent->addr, INVALID_RECLAIM_ENTRY);
        }

        reclaim_page(chunk, EH_CHUNK_SIZE_BITS);
    } else if (new_invalid > EH_CHUNK_GC_THREHOLD1 && 
                chunk_meta->node_id == tls_node_id()) {
        latest_time = READ_ONCE(chunk_meta->latest_time);
        time = sys_time_us();

        if (likely(time > latest_time))
            WRITE_ONCE(chunk_meta->latest_time, time);

        prio = READ_ONCE(chunk_meta->priority);
        acquire_fence();

        if (prio == GC_IDLE_PRIO || prio == GC_DENSE_PRIO) {
            r_ent = READ_ONCE(chunk_meta->r_entry);

            if ((prio == GC_IDLE_PRIO && r_ent == NULL) || 
                    (prio == GC_DENSE_PRIO && new_invalid > EH_CHUNK_GC_THREHOLD2 && r_ent)) {
                new_prio = (new_invalid > EH_CHUNK_GC_THREHOLD2) ? GC_SPARSE_PRIO : GC_DENSE_PRIO;
                new_r_ent = new_gc_record(new_prio);

                if (likely((void *)new_r_ent != INVALID_ADDR)) {
                    if (!cas_bool(&chunk_meta->priority, prio, new_prio) || 
                            !cas_bool(&chunk_meta->r_entry, r_ent, new_r_ent))
                        WRITE_ONCE(new_r_ent->addr, INVALID_RECLAIM_ENTRY);
                    else {
                        if (r_ent)
                            WRITE_ONCE(r_ent->addr, INVALID_RECLAIM_ENTRY);

                        cas(&new_r_ent->addr, INITIAL_RECLAIM_ENTRY, (uintptr_t)chunk);
                    }
                }
            }
        }
    }
}

void *alloc_eh_chunk(int size, bool hot) {
    struct tls_context *tls;
    struct eh_chunk_metadata *chunk_meta;
    void **__alloc, *alloc, *alloc_end, *chunk;
    BITMAP_ENT *bitmap_head, *bitmap_ent, bitmap;
    struct reclaim_entry *r_ent;
    unsigned long diff;
    RECORD_POINTER seg_free, old_seg_free;
    int ents, bits;
    unsigned invalid, new_invalid;
    GC_PRIORITY prio;
    bool already_page;

    //to dooooooooooo assert size is multiples of 8 bytes

    tls = tls_context_itself();

    __alloc = hot ? &tls->chunk_alloc_hot : &tls->chunk_alloc;
    alloc = *__alloc;

    if (likely(alloc != NULL)) {
        chunk = (void *)(EH_CHUNK_MASK & (uintptr_t)alloc);
        bitmap_head = (BITMAP_ENT *)(chunk + EH_CHUNK_BITMAP_OFFSET);

        alloc_end = alloc + size;

        if (alloc_end < (void *)bitmap_head) {//if (alloc_end <= (void *)bitmap_head) {
            *__alloc = alloc_end;

            diff = (~EH_CHUNK_MASK) & (uintptr_t)alloc;

            ents = DIV_2(diff, EH_BITS_PER_CHUNK_BITMAP_ENT);
            bitmap_ent = &bitmap_head[ents];

            diff &= ~MASK(EH_BITS_PER_CHUNK_BITMAP_ENT);

            bits = DIV_2(diff, EH_CHUNK_ALLOC_UNIT_BITS);

            while (1) {
                bitmap = READ_ONCE(*bitmap_ent);

                if (likely(cas_bool(bitmap_ent, bitmap, bitmap | EXP_2(bits))))
                    return alloc;
            }
        }

        chunk_meta = (struct eh_chunk_metadata *)(chunk + EH_CHUNK_METADATA_OFFSET);

        //to doooooooooooo chunk_meta prev_chunk and next_chunk

        while (1) {
            invalid = READ_ONCE(chunk_meta->invalid_bytes);
            new_invalid = invalid + (unsigned)((void *)bitmap_head - alloc);

            if (cas_bool(&chunk_meta->invalid_bytes, invalid, new_invalid))
                break;
        }

        if (unlikely(new_invalid == EH_CHUNK_RAW_DATA_SIZE))
            reclaim_page(chunk, EH_CHUNK_SIZE_BITS);
        else if (unlikely(new_invalid > EH_CHUNK_GC_THREHOLD1)) {
            prio = (new_invalid > EH_CHUNK_GC_THREHOLD2) ? GC_SPARSE_PRIO : GC_DENSE_PRIO;
            r_ent = new_gc_record(prio);

            if (likely((void *)r_ent != INVALID_ADDR)) {
                chunk_meta->r_entry = r_ent;

                release_fence();
                WRITE_ONCE(chunk_meta->priority, prio);

                memory_fence();
                invalid = READ_ONCE(chunk_meta->invalid_bytes);

                if (likely(invalid != EH_CHUNK_RAW_DATA_SIZE))
                    cas(&r_ent->addr, INITIAL_RECLAIM_ENTRY, (uintptr_t)chunk);
                else
                    WRITE_ONCE(r_ent->addr, INVALID_RECLAIM_ENTRY);
            } else {
                //to dooooooooooooo handle memory alloc error
            }
        } else {
            chunk_meta->r_entry = NULL;

            release_fence();
            WRITE_ONCE(chunk_meta->priority, GC_IDLE_PRIO);
        }
    }

    chunk = NULL;

    seg_free = tls->seg_free_self;

    if (seg_free == INVALID_RECORD_POINTER) {
        seg_free = READ_ONCE(tls->seg_free);

        if (seg_free == INVALID_RECORD_POINTER) {
            chunk = (void *)alloc_eh_seg(1, &already_page);

            if (unlikely(chunk == INVALID_ADDR))
                return INVALID_ADDR;
        } else {
            while (1) {
                old_seg_free = cas(&tls->seg_free, seg_free, INVALID_RECORD_POINTER);

                if (likely(old_seg_free == seg_free))
                    break;

                seg_free = old_seg_free;
            }
        }
    }

    if (!chunk) {
        chunk = (void *)page_of_record_pointer(seg_free);
        ents = ent_num_of_record_pointer(seg_free);

        seg_free = (ents == 1) ? INVALID_RECORD_POINTER : 
                    make_record_pointer(*(struct record_page **)chunk, ents - 1);

        tls->seg_free_self = seg_free;

        bitmap_head = (BITMAP_ENT *)(chunk + EH_CHUNK_BITMAP_OFFSET);
        memset(bitmap_head, 0, EH_BITMAP_ENT_PER_CHUNK * sizeof(BITMAP_ENT));
    } else
        bitmap_head = (BITMAP_ENT *)(chunk + EH_CHUNK_BITMAP_OFFSET);

    chunk_meta = (struct eh_chunk_metadata *)(chunk + EH_CHUNK_METADATA_OFFSET);

    chunk_meta->invalid_bytes = 0;
    chunk_meta->migrated_bitmap_ent = 0;
    chunk_meta->priority = GC_NO_PRIO;
    chunk_meta->node_id = tls_node_id();

    bitmap_head[0] = 1;

    *__alloc = chunk + size;

    return chunk;
}

int clean_eh_kv_chunk(void *chunk) {
    struct eh_chunk_metadata *chunk_meta;
    BITMAP_ENT *bitmap_head, *bitmap_ent, bitmap, old_bitmap, new_bitmap;
    struct eh_segment *seg;
    struct reclaim_entry *r_ent;
	EH_DIR_HEADER header;
    struct eh_dir *dir_arr[4];
    struct eh_bucket *bucket_arr[4];
    struct kv *kv_arr1[4];
    struct kv *kv_arr2[4];
    int depth_arr[4];
    unsigned invalid, total_invalid, new_invalid;
    int bucket_id, b, c, idx, count, offset, ret;

    bitmap_head = (BITMAP_ENT *)(chunk + EH_CHUNK_BITMAP_OFFSET);

    chunk_meta = (struct eh_chunk_metadata *)(chunk + EH_CHUNK_METADATA_OFFSET);

    total_invalid = 0;
    count = 0;

    for (b = 0; b < EH_BITMAP_ENT_PER_CHUNK; ++b) {
        old_bitmap = bitmap = READ_ONCE(bitmap_head[b]);

        new_bitmap = 0;

        while (bitmap) {
            idx = ctz64(bitmap);

            bitmap &= ~EXP_2(idx);

            offset = MUL_2(b, EH_BITS_PER_CHUNK_BITMAP_ENT) + MUL_2(idx, EH_CHUNK_ALLOC_UNIT_BITS);

            kv_arr1[count] = (struct kv *)(chunk + offset);
            dir_arr[count] = get_eh_dir_entry(kv_arr1[count]->prehash);
            prefech_r0(dir_arr[count]);

        #ifdef DHT_INTEGER
            invalid = KV_SIZE;
        #else
            invalid = KV_SIZE(kv_arr1[count]->key_len, kv_arr1[count]->val_len);
        #endif

            kv_arr2[count] = (struct kv *)alloc_eh_chunk(invalid, true);

            if (unlikely(kv_arr2[count] == INVALID_ADDR)) {
                //to doooooooooo handle memory allocate error
                new_bitmap |= EXP_2(idx);
                continue;
            }
            
            memcpy(kv_arr2[count], kv_arr1[count], invalid);

            count += 1;
            total_invalid += invalid;

            if (count == 4 || bitmap == 0) {
                for (c = 0; c < count; ++c) {
                    header = READ_ONCE(dir_arr[c]->header);
                    seg = eh_dir_low_seg(header);
                    depth_arr[c] = eh_dir_depth(header);

                    bucket_id = eh_seg_bucket_idx(kv_arr2[c]->prehash, depth_arr[c]);
                    bucket_arr[c] = &seg->bucket[bucket_id];
                    prefetch_eh_bucket_head(bucket_arr[c]);
                }

                for (c = 0; c < count; ++c) {
                    ret = update_eh_seg_kv_for_gc(kv_arr1[c], kv_arr2[c], 
                                    kv_arr2[c]->prehash, bucket_arr[c], depth_arr[c]);

                    if (ret) {
                        new_bitmap |= EXP_2(idx);
                    #ifdef DHT_INTEGER
                        invalid = KV_SIZE;
                    #else
                        invalid = KV_SIZE(kv_arr2[c]->key_len, kv_arr2[c]->val_len);
                    #endif
                        total_invalid -= invalid;

                        dealloc_eh_chunk(kv_arr2[c], invalid);
                    }   
                }

                count = 0;
            }
        }

        while (old_bitmap) {
            bitmap = cas(&bitmap_head[b], old_bitmap, new_bitmap);

            if (bitmap == old_bitmap)
                break;

            old_bitmap = bitmap;
            new_bitmap &= bitmap;
        }
    }


    while (1) {
        invalid = READ_ONCE(chunk_meta->invalid_bytes);

        if (unlikely(invalid == EH_CHUNK_RAW_DATA_SIZE))
            return 0;

        new_invalid = invalid + total_invalid;
        
        if (cas_bool(&chunk_meta->invalid_bytes, invalid, new_invalid))
            break;
    }

    if (likely(new_invalid == EH_CHUNK_RAW_DATA_SIZE)) {
        reclaim_page(chunk, EH_CHUNK_SIZE_BITS);
        return 0;
    }
    
    r_ent = new_gc_record(GC_LOW_PRIO);

    if (likely((void *)r_ent != INVALID_ADDR)) {
        chunk_meta->r_entry = r_ent;
        
        release_fence();
        WRITE_ONCE(chunk_meta->priority, GC_LOW_PRIO);
        
        memory_fence();
        invalid = READ_ONCE(chunk_meta->invalid_bytes);
        
        if (likely(invalid != EH_CHUNK_RAW_DATA_SIZE))
            cas(&r_ent->addr, INITIAL_RECLAIM_ENTRY, (uintptr_t)chunk);
        else {
            WRITE_ONCE(r_ent->addr, INVALID_RECLAIM_ENTRY);
            return 0;
        }
    } else {
        //to doooooooooooo handle memory alloc error
    }

    return -1;
}
