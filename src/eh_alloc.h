#ifndef __EH_ALLOC_H
#define __EH_ALLOC_H

#include "compiler.h"
#include "eh_seg.h"
#include "eh_record.h"

#define EH_ALLOC_POOL_SIZE	EXP_2(35)

#define RECORD_POOL_SIZE    EXP_2(35)

#define EH_OTHER_ALLOC_POOL_SIZE    EXP_2(EH_SEGMENT_SIZE_BITS + 5)


#define MAX_RECOED_PAGE_IN_POOL    DIV_2(RECORD_POOL_SIZE, RECORD_PAGE_SHIFT)

#define MAX_EH_SEG_IN_POOL  DIV_2(EH_ALLOC_POOL_SIZE, EH_SEGMENT_SIZE_BITS)

#define MAX_EH_SEG_FOR_OTHER_POOL  DIV_2(EH_OTHER_ALLOC_POOL_SIZE, EH_SEGMENT_SIZE_BITS)

#define MAX_EH_PREFETCH_SEG EXP_2(5)

#define EH_PREFETCH_SEG_SIZE    MUL_2(MAX_EH_PREFETCH_SEG, EH_SEGMENT_SIZE_BITS)


#ifdef  __cplusplus
extern  "C" {
#endif

typedef union page_pool {
    struct {
        u64 base_page : 36;
        u64 page_num : 28;
    };

    struct {
        u64 other_base_page : 36;
        u64 other_page_num : 27;
        u64 other_paged : 1;
    };

    u64 pool;
} PAGE_POOL;

 
void hint_eh_seg_prefault(int num);

bool is_prefault_seg_enough(int num);

struct eh_segment *alloc_eh_seg(
                    int num, 
                    bool *already_paged);

struct eh_segment *alloc_other_eh_seg(
                    int num, int nid, 
                    bool *already_paged);

struct record_page *alloc_record_page();

struct record_page *alloc_other_record_page(int nid);

int prefault_eh_seg(int tid);

#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__EH_ALLOC_H
