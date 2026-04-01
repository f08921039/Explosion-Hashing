#ifndef __EH_CHUNK_H
#define __EH_CHUNK_H

#include "compiler.h"
#include "kv.h"
#include "eh_seg.h"
#include "eh_record.h"


#define EH_CHUNK_SIZE_BITS  EH_SEGMENT_SIZE_BITS
#define EH_CHUNK_SIZE   EXP_2(EH_CHUNK_SIZE_BITS)
#define EH_CHUNK_MASK   MASK(EH_CHUNK_SIZE_BITS)

#define EH_CHUNK_ALLOC_UNIT sizeof(u64)
#define EH_CHUNK_ALLOC_UNIT_BITS LOG2(EH_CHUNK_ALLOC_UNIT)

#define EH_SIZE_PER_CHUNK_BITMAP_ENT    (BITMAP_ENT_UNIT * EH_CHUNK_ALLOC_UNIT)
#define EH_BITS_PER_CHUNK_BITMAP_ENT    LOG2(EH_SIZE_PER_CHUNK_BITMAP_ENT)

#define EH_BITMAP_ENT_PER_CHUNK \
        ((EH_CHUNK_SIZE - sizeof(struct eh_chunk_metadata)) / (EH_SIZE_PER_CHUNK_BITMAP_ENT + sizeof(BITMAP_ENT)))

#define EH_CHUNK_RAW_DATA_SIZE  (EH_SIZE_PER_CHUNK_BITMAP_ENT * EH_BITMAP_ENT_PER_CHUNK)

#define EH_CHUNK_BITMAP_OFFSET  EH_CHUNK_RAW_DATA_SIZE
#define EH_CHUNK_METADATA_OFFSET    (EH_CHUNK_BITMAP_OFFSET + EH_BITMAP_ENT_PER_CHUNK * sizeof(BITMAP_ENT))


#define EH_CHUNK_GC_THREHOLD1    DIV_2(EH_CHUNK_RAW_DATA_SIZE, 1)
#define EH_CHUNK_GC_THREHOLD2    (EH_CHUNK_GC_THREHOLD1 + DIV_2(EH_CHUNK_RAW_DATA_SIZE, 2))

#ifdef  __cplusplus
extern  "C" {
#endif

typedef enum { 
    GC_SPARSE_PRIO = 0, 
    GC_DENSE_PRIO = 1, 
	GC_LOW_PRIO = 2, 
    GC_IDLE_PRIO = 3, 
    GC_NO_PRIO = 4
} __attribute__ ((__packed__)) GC_PRIORITY;


struct eh_chunk_metadata {
    union {
        struct {
            void *prev_chunk;//to dooooooooo memory reclaim for system terminate
            void *next_chunk;//to dooooooooo memory reclaim for system terminate
        };
        struct reclaim_entry *r_entry;
    };
    u64 latest_time;
    unsigned invalid_bytes;
    unsigned short migrated_bitmap_ent;
    GC_PRIORITY priority;
    u8 node_id;
};

struct eh_gc_context {
    struct kv *kv;
    u64 hashed_key;
};

void *alloc_eh_chunk(int size, bool hot);
void dealloc_eh_chunk(void *addr, int size);
int clean_eh_kv_chunk(void *chunk);

#ifdef DHT_INTEGER
static inline 
struct kv *alloc_eh_kv(bool hot) {
    return (struct kv *)alloc_eh_chunk(KV_SIZE, hot);
}

static inline 
void dealloc_eh_kv(struct kv *kv) {
    dealloc_eh_chunk((void *)kv, KV_SIZE);
}
#else
static inline 
struct kv *alloc_eh_kv(int key_len, int val_len, bool hot) {
    return (struct kv *)alloc_eh_chunk(KV_SIZE(key_len, val_len), hot);
}

static inline 
void dealloc_eh_kv(struct kv *kv) {
    dealloc_eh_chunk((void *)kv, KV_SIZE(kv->key_len, kv->val_len));
}
#endif

static inline 
int eh_gc_kv(struct reclaim_entry *r_ent, GC_PRIORITY prio) {
    void *chunk;
    struct eh_chunk_metadata *chunk_meta;

    chunk = (void *)READ_ONCE(r_ent->addr);

    if (unlikely(chunk == (void *)INITIAL_RECLAIM_ENTRY))
        return 1;

    if (unlikely(chunk == (void *)INVALID_RECLAIM_ENTRY))
        return 0;

    chunk_meta = (struct eh_chunk_metadata *)(chunk + EH_CHUNK_METADATA_OFFSET);

    if (unlikely(!cas_bool(&chunk_meta->priority, prio, GC_NO_PRIO)))
        return 0;

    WRITE_ONCE(chunk_meta->r_entry, NULL);

    release_fence();
    WRITE_ONCE(r_ent->addr, INVALID_RECLAIM_ENTRY);

    return clean_eh_kv_chunk(chunk);
}

#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__EH_CHUNK_H
