#include "eh_dir.h"
#include "eh_alloc.h"
#include "per_thread.h"
//#include "background.h"

struct eh_dir *expand_eh_directory(
            EH_CONTEXT *context,
            EH_CONTEXT c_val,
            struct eh_dir *dir_head,
            int old_depth, 
            int new_depth) {
    struct eh_dir *new_dir_head, *old_dir_end, *dir_new, *dir_old;
    EH_DIR_HEADER header, mig_header;
    EH_CONTEXT *migrate_context;
    EH_CONTEXT mig_c_val, upt_c_val;
    //struct per_node_context *per_nc;
    unsigned long num, i;
    int migs, _migs, mig_offset, per_mig, cts, depth, diff;

    migrate_context = __get_eh_migrating_context(context);
    mig_c_val = READ_ONCE(*migrate_context);
    
    header = READ_ONCE(dir_head->header);
    dir_old = dir_head;
    
    diff = new_depth - old_depth;
    
    if (c_val == mig_c_val) {
        new_dir_head = (struct eh_dir *)malloc_page_aligned(
                  MUL_2(EH_DIR_HEADER_SIZE, new_depth - EH_GROUP_BITS));
                
        if (unlikely((void *)new_dir_head == INVALID_ADDR))
            return (struct eh_dir *)INVALID_ADDR;

        depth = eh_dir_depth(header);
        
        if (new_depth < 15) {
            _migs = EH_CONTEXT_MIGRATES;
            cts = 1;
        } else if (depth < EH_CONTEXT_MIGRATES_BITS + EH_GROUP_BITS) {
            _migs = EXP_2((EH_GROUP_BITS + EH_CONTEXT_MIGRATES_BITS) - depth);
            cts = 1 + EH_CONTEXT_MIGRATES - _migs;
        } else {
            _migs = 1;
            cts = EH_CONTEXT_MIGRATES;
        }
            
        upt_c_val = set_eh_dir_context(new_dir_head, new_depth, _migs, cts);
        mig_c_val = cas(migrate_context, c_val, upt_c_val);
        
        if (mig_c_val != c_val)
            reclaim_page(new_dir_head, new_depth + EH_DIR_HEADER_SIZE_BITS - EH_GROUP_BITS);
        else {
            migs = 0;
            mig_offset = 0;
        }
    }
    
    while (mig_c_val != c_val) {
        c_val = mig_c_val;
        new_dir_head = extract_eh_dir(mig_c_val);
        migs = eh_migrates(mig_c_val);
        cts = eh_count_downs(mig_c_val);
           
        if (migs == EH_CONTEXT_MIGRATES || new_depth != eh_depth(mig_c_val))
            return NULL;
            
        mig_offset = SHIFT_OF(migs, old_depth - EH_GROUP_BITS - EH_CONTEXT_MIGRATES_BITS);
        dir_old = &dir_head[mig_offset];
        
        header = READ_ONCE(dir_old->header);
        depth = eh_dir_depth(header);
        
        if (depth < EH_CONTEXT_MIGRATES_BITS + EH_GROUP_BITS) {
            _migs = EXP_2((EH_CONTEXT_MIGRATES_BITS + EH_GROUP_BITS) - depth);
            cts -= (_migs - 1);
        } else
            _migs = 1;
            
        upt_c_val = set_eh_dir_context(new_dir_head, new_depth, migs + _migs, cts);
        mig_c_val = cas(migrate_context, c_val, upt_c_val);
    }
    
    while (1) {
        per_mig = SHIFT_OF(_migs, old_depth - EH_GROUP_BITS - EH_CONTEXT_MIGRATES_BITS);
        dir_new = &new_dir_head[SHIFT_OF(mig_offset, diff)];
        old_dir_end = &dir_old[per_mig];
        
        while (dir_old != old_dir_end) {
            header = READ_ONCE(dir_old->header);
            depth = eh_dir_depth(header);
            
            mig_header = set_eh_dir_migrate(header);

            if (unlikely(!cas_bool(&dir_old->header, header, mig_header)))
                continue;

            header = cancel_eh_dir_spliting(header);

            num = EXP_2(new_depth - depth);

            for (i = 0; i < num; ++i)
                dir_new[i].header = header;

            dir_new += num;
            dir_old += DIV_2(num, diff);
        }
        
        mig_c_val = READ_ONCE(*migrate_context);
        
        while (1) {
            migs = eh_migrates(mig_c_val);
            cts = eh_count_downs(mig_c_val) - 1;
            
            if (migs == EH_CONTEXT_MIGRATES) {
                upt_c_val = set_eh_dir_context(new_dir_head, new_depth, EH_CONTEXT_MIGRATES, cts);

                if (cts == 0) {
                    release_fence();
                    WRITE_ONCE(*context, upt_c_val);
                    WRITE_ONCE(*migrate_context, upt_c_val);

                    reclaim_page(dir_head, old_depth + EH_DIR_HEADER_SIZE_BITS - EH_GROUP_BITS);

                    return new_dir_head;
                }
                
                c_val = mig_c_val;
                mig_c_val = cas(migrate_context, c_val, upt_c_val);
                    
                if (mig_c_val == c_val)
                    return NULL;
            } else {
                mig_offset = SHIFT_OF(migs, old_depth - EH_GROUP_BITS - EH_CONTEXT_MIGRATES_BITS);
                dir_old = &dir_head[mig_offset];
                
                header = READ_ONCE(dir_old->header);
                depth = eh_dir_depth(header);
        
                if (depth < EH_CONTEXT_MIGRATES_BITS + EH_GROUP_BITS) {
                    _migs = EXP_2((EH_CONTEXT_MIGRATES_BITS + EH_GROUP_BITS) - depth);
                    cts -= (_migs - 1);
                } else
                    _migs = 1;
                
                upt_c_val = set_eh_dir_context(new_dir_head, new_depth, migs + _migs, cts);
                
                c_val = mig_c_val;
                mig_c_val = cas(migrate_context, c_val, upt_c_val);
                
                if (mig_c_val == c_val)
                    break;
            }
        }
    }

    return NULL;
}


int split_eh_directory(
        struct eh_dir *dir,
        struct eh_segment *new_seg, 
        int l_depth, 
        int g_depth) {
    EH_DIR_HEADER header, header1, header2, header2_s;
    unsigned long i, half_num;
    struct eh_dir *dir_half;
    int node_id;

    header = READ_ONCE(dir->header);

    if (unlikely(eh_dir_cannot_split(header) || 
                    l_depth != eh_dir_depth(header)))
        return -1;

    node_id = eh_dir_node(header);
    
    header2 = make_eh_dir_header(&new_seg[1], l_depth + 1, node_id);
    header1 = make_eh_dir_header(&new_seg[0], l_depth + 1, node_id);

    header2_s = set_eh_dir_spliting(header2);

    half_num = EXP_2(g_depth - l_depth - 1);

    dir_half = &dir[half_num];

    for (i = 1; i < half_num; ++i)
        WRITE_ONCE(dir[i].header, header1);

    for (i = 0; i < half_num; ++i)
        WRITE_ONCE(dir_half[i].header, header2_s);

    if (unlikely(!cas_bool(&dir->header, header, header1)))
        return -1;

    cas(&dir_half->header, header2_s, header2);

    return 0;
}
