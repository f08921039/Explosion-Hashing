#ifndef __KV_H
#define __KV_H

#include "compiler.h"

#define PREHASH_KEY_LEN 8
#define PREHASH_KEY_BITS    MUL_2(PREHASH_KEY_LEN, 3)

//#define PREHASH_KEY(key, bit1, bit2) ((key) & INTERVAL(bit1, bit2))


#ifdef  __cplusplus
extern  "C" {
#endif


#ifdef DHT_INTEGER
    typedef u64 KEY_ITEM;
    typedef u64 VALUE_ITEM;

    #define KV_SIZE sizeof(struct kv)
#else
    typedef struct {
        void *key;
        int len;
    } KEY_ITEM;

    typedef struct {
        void *value;
        int len;
    } VALUE_ITEM;

    #define KV_SIZE(key_len, val_len)   (sizeof(struct kv) + (key_len) + (val_len))
#endif


struct kv {
    u64 prehash;
#ifdef DHT_INTEGER
    KEY_ITEM key;
    VALUE_ITEM value;
#else
    unsigned int key_len;
    unsigned int val_len;
    u8 kv[0];
#endif

};//__attribute__((aligned(8)));


#ifdef DHT_INTEGER
static inline 
u64 get_kv_prehash64(struct kv *kv) {
    return kv->prehash;
}

static inline 
int __compare_kv_key(struct kv *kv, KEY_ITEM key) {
    return (kv->key != key);
}

static inline 
int compare_kv_key(struct kv *kv1, struct kv *kv2) {
    return (kv1->key != kv2->key);
}

static inline 
void __copy_kv_val(struct kv *kv_src, VALUE_ITEM *val_buf) {
    *val_buf = kv_src->value;
}

static inline 
void copy_kv_val(struct kv *kv_dest, struct kv *kv_src) {
    kv_dest->value = kv_src->value;
}

static inline 
void init_kv(
        struct kv *kv, 
        KEY_ITEM key, 
        VALUE_ITEM val,
        u64 prehash) {
    kv->prehash = prehash;

    kv->key = key;
    kv->value = val;
}

#else
static inline 
u64 get_kv_prehash64(struct kv *kv) {
    return kv->prehash;
}

static inline 
int __compare_kv_key(struct kv *kv, KEY_ITEM key) {
    if (kv->key_len != key.len)
        return 1;

    return memcmp(&kv->kv[0], key.key, key.len);
}

static inline 
int compare_kv_key(struct kv *kv1, struct kv *kv2) {
    if (kv1->prehash != kv2->prehash || kv1->key_len != kv2->key_len)
        return 1;

    return memcmp(&kv1->kv[0], &kv2->kv[0], kv1->key_len);
}

static inline 
int __copy_kv_val(struct kv *kv_src, VALUE_ITEM *val_buf) {
    int len = ((kv_src->val_len < val_buf->len) ? kv_src->val_len : val_buf->len);

    memcpy(val_buf->value, &kv_src->kv[kv_src->key_len], len);

    if (kv_src->val_len > len)
        return len - kv_src->val_len;

    return len;
}

static inline 
int copy_kv_val(struct kv *kv_dest, struct kv *kv_src) {
    int len = ((kv_src->val_len < kv_dest->val_len) ? kv_src->val_len : kv_dest->val_len);

    memcpy(&kv_dest->kv[kv_dest->key_len], &kv_src->kv[kv_src->key_len], len);

    if (kv_src->val_len > kv_dest->val_len)
        return kv_dest->val_len - kv_src->val_len;
    
    return len;
}


static inline 
void init_kv(
        struct kv *kv, 
        KEY_ITEM key, 
        VALUE_ITEM val,
        u64 prehash) {
    kv->prehash = prehash;
    kv->key_len = key.len;
    kv->val_len = val.len;

    memcpy(&kv->kv[0], key.key, key.len);
    memcpy(&kv->kv[key.len], val.value, val.len);
}
#endif

#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__KV_H
