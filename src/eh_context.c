#include "eh_context.h"
#include "eh_dir.h"
#include "eh_seg.h"

EH_CONTEXT eh_group[EH_GROUP_NUM] __attribute__ ((aligned (CACHE_LINE_SIZE)));

EH_CONTEXT eh_migrating_group[EH_GROUP_NUM] __attribute__ ((aligned (CACHE_LINE_SIZE)));

void release_eh_structure() {
    //to doooooooooooooooooooooo
}

int init_eh_structure(int nodes, int *node_map) {
    struct eh_segment *seg;
    struct eh_dir *dir;
    EH_DIR_HEADER header;
    int node_bits, redundant_node, segs_per_node, l_depth, node_ent;
    int i, j, k, n, r, size, d_size, ac, acc, ent_num;

    node_bits = LOG2(nodes);
    redundant_node = nodes - EXP_2(node_bits);
    node_bits += (redundant_node != 0);

    node_ent = ((node_bits > EH_GROUP_BITS) ? 
                            EXP_2(INITIAL_EH_G_DEPTH - EH_GROUP_BITS) : 
                                    EXP_2(INITIAL_EH_G_DEPTH - node_bits));

    if (node_bits > INITIAL_EH_SEG_BITS) {
        segs_per_node = 1;
        l_depth = node_bits;
    } else {
        segs_per_node = EXP_2(INITIAL_EH_SEG_BITS - node_bits);
        l_depth = INITIAL_EH_SEG_BITS;
    }

    memset(&eh_group[0], EMPTY_EH_CONTEXT, EH_GROUP_NUM * sizeof(EH_CONTEXT));
    memset(&eh_migrating_group[0], EMPTY_EH_CONTEXT, EH_GROUP_NUM * sizeof(EH_CONTEXT));

    acc = ac = 0;
    d_size = MUL_2(node_ent, EH_DIR_HEADER_SIZE_BITS);
    ent_num = EXP_2(INITIAL_EH_G_DEPTH - l_depth);

    for (i = 0; i < EXP_2(node_bits); ++i) {
        n = node_map[i % nodes];

        if (ac % node_ent == 0) {
            dir = (struct eh_dir *)alloc_node_page(d_size, n);

            if (unlikely(dir == (struct eh_dir *)INVALID_ADDR)) {
                release_eh_structure();
                return -1;
            }

            memset(dir, 0, d_size);
            ac = 0;
        }

        size = /*3 * */MUL_2(segs_per_node, EH_SEGMENT_SIZE_BITS);
        seg = (struct eh_segment *)alloc_node_aligned_page(size, n, EH_SEGMENT_SIZE_BITS);//alloc_node_page(size, n);

        if (unlikely(seg == (struct eh_segment *)INVALID_ADDR)) {
            release_eh_structure();
            return -1;
        }

        memset(seg, 0, size);

        for (j = 0; j < segs_per_node; ++j) {
            if (ac % INITIAL_EH_DIR_ENT_PER_GROUP == 0) {
                r = (acc + ac) / INITIAL_EH_DIR_ENT_PER_GROUP;
                eh_group[r] = set_eh_dir_context(&dir[ac], INITIAL_EH_G_DEPTH, EH_CONTEXT_MIGRATES, 0);
                eh_migrating_group[r] = eh_group[r];
            }

            header = make_eh_dir_header(seg, l_depth, n);

            /*for (k = 0; k < EH_BUCKET_NUM; ++k)
                seg->bucket[k].header = set_eh_seg_low(&seg[1]);*/

            seg += 1;//seg += 3;

            for (k = 0; k < ent_num; ++k)
                dir[ac++].header = header;
        }
        
        acc += ac;
    }

    memory_fence();
    return 0;
}
