#include "eh_seg.h"
#include "eh_rehash.h"
#include "eh_alloc.h"
#include "eh_chunk.h"
#include "per_thread.h"

//#define EH_ADV_SPLIT_THREHOLD	DIV_2(EH_SLOT_NUM_PER_BUCKET * 15, 4)
#define EH_ADV_SPLIT_THREHOLD	DIV_2(EH_SLOT_NUM_PER_BUCKET * 7, 3)
#define EH_BOOST_SPLIT_THREHOLD	DIV_2(EH_SLOT_NUM_PER_BUCKET * 11, 3)
#define EH_EMERGENCY_SPLIT_THREHOLD	DIV_2(EH_SLOT_NUM_PER_BUCKET * 23, 4)
#define EH_URGENT_SPLIT_THREHOLD	DIV_2(EH_SLOT_NUM_PER_BUCKET * 27, 4)
//#define EH_EMERGENCY_SPLIT_THREHOLD	DIV_2(EH_SLOT_NUM_PER_BUCKET * 11, 3)

//#define EH_ADV_PREALLOCATE_THREHOLD1	(EH_ADV_SPLIT_THREHOLD - 4)
//#define EH_ADV_PREALLOCATE_THREHOLD1	DIV_2(EH_SLOT_NUM_PER_BUCKET * 3, 2)
#define EH_ADV_PREALLOCATE_THREHOLD1	DIV_2(EH_SLOT_NUM_PER_BUCKET * 7, 3)
//#define EH_ADV_PREALLOCATE_THREHOLD2	(EH_EMERGENCY_SPLIT_THREHOLD - 7)
#define EH_ADV_PREALLOCATE_THREHOLD2	DIV_2(EH_SLOT_NUM_PER_BUCKET * 9, 3)

typedef u64 EH_SLOT_RECORD;

#define EH_RECORD_DEPTH_BIT_START	0
#define EH_RECORD_DEPTH_BIT_END	(EH_RECORD_DEPTH_BIT_START + EH_DEPTH_BITS - 1)

#define EH_RECORD_BUCKET_BIT_START	EH_DEPTH_BITS
#define EH_RECORD_BUCKET_BIT_END	(VALID_POINTER_BITS - 1)

#define EH_RECORD_LAYER_BIT_START	VALID_POINTER_BITS
#define EH_RECORD_LAYER_BIT_END	(EH_RECORD_LAYER_BIT_START + EH_DEPTH_BITS - 1)

#define EH_RECORD_ID_BIT_START	(EH_RECORD_LAYER_BIT_END + 1)
#define EH_RECORD_ID_BIT_END	(POINTER_BITS - 1)

#define EH_RECORD_BUCKET_MASK	\
			INTERVAL(EH_RECORD_BUCKET_BIT_START, EH_RECORD_BUCKET_BIT_END)

#define INVALID_EH_SLOT_RECORD	0UL

#define eh_record_bucket(record)	((record) & EH_RECORD_BUCKET_MASK)
#define eh_record_id(record)	\
			INTERVAL_OF(record, EH_RECORD_ID_BIT_START, EH_RECORD_ID_BIT_END)
#define eh_record_depth(record)	\
		INTERVAL_OF(record, EH_RECORD_DEPTH_BIT_START, EH_RECORD_DEPTH_BIT_END)
#define eh_record_layer(record)	\
		INTERVAL_OF(record, EH_RECORD_LAYER_BIT_START, EH_RECORD_LAYER_BIT_END)


#define set_eh_slot_record(bucket, slot_id, depth, layer)	\
				((uintptr_t)(bucket) |	\
				SHIFT_OF(layer, EH_RECORD_LAYER_BIT_START) |	\
				SHIFT_OF(slot_id, EH_RECORD_ID_BIT_START) |	\
				SHIFT_OF(depth, EH_RECORD_DEPTH_BIT_START))

struct eh_slot_record_pair {
	EH_SLOT_RECORD record[2];
	EH_BUCKET_SLOT slot[2];
};

__attribute__((always_inline))
static struct eh_segment *retrieve_eh_high_segment(EH_BUCKET_HEADER header) {
	if (eh_seg_low(header))
		return eh_next_high_seg(header);

	return NULL;
}


__attribute__((always_inline))
static void record_bucket_initial_traversal(struct eh_slot_record_pair *rec_pair) {
	rec_pair->record[0] = rec_pair->record[1] = INVALID_EH_SLOT_RECORD;
}

__attribute__((always_inline))
static bool check_bucket_traversal(EH_SLOT_RECORD record) {
	return (record != INVALID_EH_SLOT_RECORD);
}

__attribute__((always_inline))
static void record_bucket_traversal(
			struct eh_slot_record_pair *rec_pair, 
			EH_BUCKET_SLOT new_slot,
			struct eh_bucket *bucket, 
			int slot_id, int l_depth, 
			int layer) {
	EH_SLOT_RECORD r = set_eh_slot_record(bucket, slot_id, l_depth, layer);

	if (!check_bucket_traversal(rec_pair->record[0])) {
		rec_pair->record[0] = r;
		rec_pair->slot[0] = new_slot;
	} else if (!check_bucket_traversal(rec_pair->record[1])) {
		rec_pair->record[1] = r;
		rec_pair->slot[1] = new_slot;
	}
}

enum eh_op_type {
	EH_INSERT, 
	EH_LOOKUP, 
	EH_UPDATE, 
	EH_DELETE
};

enum eh_slot_state {
	SUCCESS_SLOT = 0, 
	INVALID_SLOT = -1, 
	DELETED_SLOT = -2, 
	EXISTED_SLOT = -3,  
	UNMATCH_SLOT = -6, 
	LACK_SLOT = -7
};

__attribute__((always_inline))
static enum eh_slot_state compare_eh_slot(
					KEY_ITEM key,
					struct kv **slot_kv_ptr, 
					EH_BUCKET_SLOT slot_val) {
	struct kv *slot_kv;

	if (likely(!eh_slot_invalid(slot_val))) {
		slot_kv = eh_slot_kv_addr(slot_val);

		if (__compare_kv_key(slot_kv, key) == 0) {
			if (slot_kv_ptr != NULL)
				*slot_kv_ptr = slot_kv;

			if (eh_slot_deleted(slot_val))
				return DELETED_SLOT;

			return EXISTED_SLOT;
		}

		return UNMATCH_SLOT;
	}

	return INVALID_SLOT;
}

static enum eh_slot_state __check_unique_eh_slot(
					struct kv *kv, 
					KEY_ITEM key, 
					u64 hashed_key, 
					EH_BUCKET_SLOT *slot_addr, 
					struct eh_slot_record_pair *rec_pair) {
	EH_BUCKET_SLOT slot_val, new_slot_val, old_slot_val;
	EH_BUCKET_HEADER header;
	struct eh_bucket *bucket;
	struct eh_segment *next_seg;
	struct kv *old_kv;
	u64 f1, f2;
	enum eh_slot_state state;
	int depth, slot_id, bucket_id;
	bool first;

	first = true;

	bucket = (struct eh_bucket *)eh_record_bucket(rec_pair->record[0]);
	slot_id = eh_record_id(rec_pair->record[0]);
	depth = eh_record_depth(rec_pair->record[0]);

	if (kv)//update
		new_slot_val = rec_pair->slot[0];

check_unique_eh_slot_for_next_segment :
	f1 = hashed_key_fingerprint(hashed_key, depth, 16);

	for (; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
		slot_val = READ_ONCE(bucket->kv[slot_id]);

		if (eh_slot_free(slot_val))
			return EXISTED_SLOT;

		if (eh_slot_end(slot_val))
			break;

		f2 = eh_slot_fingerprint(slot_val, 16);

		if (f1 == f2) {
			if (slot_addr == &bucket->kv[slot_id])
				return EXISTED_SLOT;

			while (1) {
				state = compare_eh_slot(key, &old_kv, slot_val);

				if (state == UNMATCH_SLOT || state == INVALID_SLOT)
					break;

				if (state == DELETED_SLOT)
					return DELETED_SLOT;

				if (kv == NULL)//delete
					new_slot_val = set_eh_slot_deleted(slot_val);

				old_slot_val = cas(&bucket->kv[slot_id], slot_val, new_slot_val);

				if (unlikely(old_slot_val != slot_val)) {
					slot_val = old_slot_val;
					continue;
				}

				if (kv)//update
					dealloc_eh_kv(old_kv);

				return SUCCESS_SLOT;
			}
		}
	}

	if (first) {
		if (likely(check_bucket_traversal(rec_pair->record[1]))) {
			bucket = (struct eh_bucket *)eh_record_bucket(rec_pair->record[1]);
			slot_id = eh_record_id(rec_pair->record[1]);
			depth = eh_record_depth(rec_pair->record[1]);

			if (kv)//update
				new_slot_val = rec_pair->slot[1];

			first = false;
			goto check_unique_eh_slot_for_next_segment;
		}

		return EXISTED_SLOT;
	}

	header = READ_ONCE(bucket->header);
	next_seg = retrieve_eh_high_segment(header);

	if (next_seg && likely(!eh_bucket_stayed(header))) {
		slot_id = 0;
		bucket_id = eh_seg2_bucket_idx(hashed_key, depth);
		bucket = &next_seg->bucket[bucket_id];
		depth += 1;

		if (kv) {//update
			f1 = hashed_key_fingerprint(hashed_key, depth, 18);
			new_slot_val = make_eh_ext2_slot(f1, kv);
		}

		goto check_unique_eh_slot_for_next_segment;
	}

	return EXISTED_SLOT;
}

__attribute__((always_inline))
static enum eh_slot_state check_unique_eh_slot(
					struct kv *kv, 
					KEY_ITEM key, 
					u64 hashed_key, 
					EH_BUCKET_SLOT *slot_addr, 
					struct eh_slot_record_pair *rec_pair) {
	EH_BUCKET_SLOT slot_val;
	struct eh_bucket *bucket;
	int slot_id;

	if (!check_bucket_traversal(rec_pair->record[0]))
		return EXISTED_SLOT;

	bucket = (struct eh_bucket *)eh_record_bucket(rec_pair->record[0]);
	slot_id = eh_record_id(rec_pair->record[0]);

	slot_val = READ_ONCE(bucket->kv[slot_id]);

	if (likely(eh_slot_free(slot_val)))
		return EXISTED_SLOT;

	return __check_unique_eh_slot(kv, key, hashed_key, slot_addr, rec_pair);
}

__attribute__((always_inline))
static enum eh_slot_state update_eh_matched_slot(
						struct kv *kv, 
						KEY_ITEM key, 
						u64 hashed_key, 
						EH_BUCKET_SLOT slot_val, 
						EH_BUCKET_SLOT *slot_addr,
						EH_BUCKET_SLOT new_slot, 
						struct eh_slot_record_pair *rec_pair) {
	EH_BUCKET_SLOT old_slot;
	struct kv *old_kv;
	enum eh_slot_state state;

	while (1) {
		state = compare_eh_slot(key, &old_kv, slot_val);

		if (state == EXISTED_SLOT) {
			state = check_unique_eh_slot(kv, key, hashed_key, slot_addr, rec_pair);

			if (likely(state == EXISTED_SLOT)) {
				old_slot = cas(slot_addr, slot_val, new_slot);

				if (likely(old_slot == slot_val)) {
					dealloc_eh_kv(old_kv);
					return SUCCESS_SLOT;
				}

				record_bucket_initial_traversal(rec_pair);
				slot_val = old_slot;
				continue;
			}
		}

		break;
	}

	return state;
}

__attribute__((always_inline))
static enum eh_slot_state delete_eh_matched_slot(
						KEY_ITEM key,
						u64 hashed_key, 
						EH_BUCKET_SLOT slot_val, 
						EH_BUCKET_SLOT *slot_addr,
						struct eh_slot_record_pair *rec_pair) {
	EH_BUCKET_SLOT old_slot, new_slot;
	enum eh_slot_state state;

	while (1) {
		state = compare_eh_slot(key, NULL, slot_val);

		if (state == EXISTED_SLOT) {
			state = check_unique_eh_slot(NULL, key, hashed_key, slot_addr, rec_pair);

			if (likely(state == EXISTED_SLOT)) {
				new_slot = set_eh_slot_deleted(slot_val);

				old_slot = cas(slot_addr, slot_val, new_slot);

				if (likely(old_slot == slot_val))
					return SUCCESS_SLOT;

				record_bucket_initial_traversal(rec_pair);
				slot_val = old_slot;
				continue;
			}
		}

		break;
	}

	return state;
}

__attribute__((always_inline))
static enum eh_slot_state lookup_eh_matched_slot(
						KEY_ITEM key, 
						struct kv **slot_kv_ptr, 
						EH_BUCKET_SLOT slot_val) {
	return compare_eh_slot(key, slot_kv_ptr, slot_val);
}

__attribute__((always_inline))
static enum eh_slot_state insert_eh_matched_slot(
						KEY_ITEM key, 
						EH_BUCKET_SLOT slot_val, 
						EH_BUCKET_SLOT *slot_addr,
						EH_BUCKET_SLOT new_slot) {
	EH_BUCKET_SLOT old_slot;
	struct kv *old_kv;
	enum eh_slot_state state;

	while (1) {
		state = compare_eh_slot(key, &old_kv, slot_val);

		if (state == DELETED_SLOT) {
			old_slot = cas(slot_addr, slot_val, new_slot);

			if (likely(old_slot == slot_val)) {
				dealloc_eh_kv(old_kv);
				return SUCCESS_SLOT;
			}

			slot_val = old_slot;
			continue;
		}

		break;
	}

	return state;
}

static enum eh_slot_state update_eh_defered_bucket(
					struct eh_bucket *bucket, 
					KEY_ITEM key, 
					EH_BUCKET_SLOT new_slot, 
					u64 fingerprint) {
	EH_BUCKET_SLOT slot, old_slot; 
	EH_BUCKET_SLOT *slot_addr;
	u64 f2;
	struct kv *old_kv;
	enum eh_slot_state state;
	int i;

	for (i = 0; i < EH_SLOT_NUM_PER_DEFER_BUCKET; ++i) {
		slot_addr = &bucket->kv[EH_SLOT_NUM_PER_BUCKET - 1 - i];
		slot = READ_ONCE(*slot_addr);

		if (eh_slot_free(slot) || unlikely(eh_slot_end(slot)))
			return UNMATCH_SLOT;

		f2 = eh_slot_fingerprint(slot, 16);

		if (unlikely(fingerprint == f2)) {
			state = compare_eh_slot(key, &old_kv, slot);

			while (state == EXISTED_SLOT) {
				old_slot = cas(slot_addr, slot, new_slot);

				if (likely(old_slot == slot)) {
					dealloc_eh_kv(old_kv);
					return SUCCESS_SLOT;
				}

				slot = old_slot;
				
				if (unlikely(eh_slot_invalid(slot)))
					return INVALID_SLOT;

				if (eh_slot_deleted(slot))
					return DELETED_SLOT;
			}

			if (state == DELETED_SLOT)
				return DELETED_SLOT;

			if (unlikely(state == INVALID_SLOT))
				return INVALID_SLOT;
		}
	}

	return UNMATCH_SLOT;
}

static enum eh_slot_state delete_eh_defered_bucket(
					struct eh_bucket *bucket, 
					KEY_ITEM key, 
					u64 fingerprint) {
	EH_BUCKET_SLOT slot, old_slot, new_slot; 
	EH_BUCKET_SLOT *slot_addr;
	u64 f2;
	enum eh_slot_state state;
	int i;

	for (i = 0; i < EH_SLOT_NUM_PER_DEFER_BUCKET; ++i) {
		slot_addr = &bucket->kv[EH_SLOT_NUM_PER_BUCKET - 1 - i];
		slot = READ_ONCE(*slot_addr);

		if (eh_slot_free(slot) || unlikely(eh_slot_end(slot)))
			return UNMATCH_SLOT;

		f2 = eh_slot_fingerprint(slot, 16);

		if (unlikely(fingerprint == f2)) {
			state = compare_eh_slot(key, NULL, slot);

			while (state == EXISTED_SLOT) {
				new_slot = set_eh_slot_deleted(slot);

				old_slot = cas(slot_addr, slot, new_slot);

				if (likely(old_slot == slot))
					return SUCCESS_SLOT;

				slot = old_slot;
				
				if (unlikely(eh_slot_invalid(slot)))
					return INVALID_SLOT;

				if (eh_slot_deleted(slot))
					return DELETED_SLOT;
			}

			if (state == DELETED_SLOT)
				return DELETED_SLOT;

			if (unlikely(state == INVALID_SLOT))
				return INVALID_SLOT;
		}
	}

	return UNMATCH_SLOT;
}

static enum eh_slot_state lookup_eh_defered_bucket(
					struct eh_bucket *bucket, 
					KEY_ITEM key, 
					struct kv **slot_kv_ptr, 
					u64 fingerprint) {
	EH_BUCKET_SLOT slot, *slot_addr;
	u64 f2;
	enum eh_slot_state state;
	int i;

	for (i = 0; i < EH_SLOT_NUM_PER_DEFER_BUCKET; ++i) {
		slot_addr = &bucket->kv[EH_SLOT_NUM_PER_BUCKET - 1 - i];
		slot = READ_ONCE(*slot_addr);

		if (eh_slot_free(slot) || unlikely(eh_slot_end(slot)))
			return UNMATCH_SLOT;

		f2 = eh_slot_fingerprint(slot, 16);

		if (unlikely(fingerprint == f2)) {
			state = lookup_eh_matched_slot(key, slot_kv_ptr, slot);

			if (state == EXISTED_SLOT)
				return EXISTED_SLOT;

			if (state == DELETED_SLOT)
				return DELETED_SLOT;

			if (unlikely(state == INVALID_SLOT))
				return INVALID_SLOT;
		}
	}

	return UNMATCH_SLOT;
}

static enum eh_slot_state insert_eh_defered_bucket(
					struct eh_bucket *bucket, 
					KEY_ITEM key, 
					EH_BUCKET_SLOT new_slot, 
					u64 fingerprint, 
					bool invalid) {
	EH_BUCKET_SLOT slot, *slot_addr;
	u64 f2;
	enum eh_slot_state state;
	int i;

	for (i = 0; i < EH_SLOT_NUM_PER_DEFER_BUCKET; ++i) {
		slot_addr = &bucket->kv[EH_SLOT_NUM_PER_BUCKET - 1 - i];
		slot = READ_ONCE(*slot_addr);

		if (eh_slot_free(slot)) {
			if (unlikely(invalid)) {
				slot = cas(slot_addr, FREE_EH_SLOT, END_EH_SLOT);
				state = LACK_SLOT;
			} else {
				slot = cas(slot_addr, FREE_EH_SLOT, new_slot);
				state = SUCCESS_SLOT;
			}

			if (likely(eh_slot_free(slot)))
				return state;
		}

		if (unlikely(eh_slot_end(slot)))
			return LACK_SLOT;

		f2 = eh_slot_fingerprint(slot, 16);

		if (unlikely(fingerprint == f2)) {
			state = insert_eh_matched_slot(key, slot, slot_addr, new_slot);

			if (state == EXISTED_SLOT)
				return EXISTED_SLOT;

			if (state == SUCCESS_SLOT)
				return SUCCESS_SLOT;

			if (unlikely(state == INVALID_SLOT))
				return LACK_SLOT;
		}
	}

	return LACK_SLOT;
}

static int __append_eh_slot(
			struct eh_slot_record_pair *rec_pair, 
			struct kv *kv, 
			KEY_ITEM key, 
			u64 hashed_key) {
	EH_BUCKET_SLOT new_slot, tmp_slot;
	struct eh_bucket *bucket;
	struct eh_segment *next_seg;
	EH_BUCKET_HEADER header;
	u64 f1, f2;
	enum eh_slot_state state;
	int slot_id, bucket_id, depth, layer;
	bool first, invalid;

retry_append_eh_slot :
	first = true;
	invalid = false;

	bucket = (struct eh_bucket *)eh_record_bucket(rec_pair->record[0]);
	slot_id = eh_record_id(rec_pair->record[0]);
	depth = eh_record_depth(rec_pair->record[0]);
	layer = eh_record_layer(rec_pair->record[0]);

	new_slot = rec_pair->slot[0];

append_eh_slot_next_segment :
	f1 = eh_slot_fingerprint(new_slot, 16);

	for (; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
		tmp_slot = READ_ONCE(bucket->kv[slot_id]);
		//acquire_fence();

		if (likely(eh_slot_free(tmp_slot))) {
			if (unlikely(invalid)) {
				record_bucket_traversal(rec_pair, new_slot, bucket, 
				                                    slot_id, depth, layer);
				break;
			} else {
				tmp_slot = cas(&bucket->kv[slot_id], FREE_EH_SLOT, new_slot);

				if (likely(eh_slot_free(tmp_slot)))
					return EH_SLOT_NUM_PER_BUCKET * layer + slot_id;
			}
		}

		if (unlikely(eh_slot_end(tmp_slot)))
			break;

		f2 = eh_slot_fingerprint(tmp_slot, 16);

		if (unlikely(f1 == f2)) {
			state = insert_eh_matched_slot(key, tmp_slot, &bucket->kv[slot_id], new_slot);

			if (state == EXISTED_SLOT)
				return EXISTED_SLOT;

			if (state == SUCCESS_SLOT)
				return 0;

			if (state == INVALID_SLOT && !invalid) {
				invalid = true;
				first = false;
				record_bucket_initial_traversal(rec_pair);
			}
		}
	}


	if (first) {
		first = false;

		if (check_bucket_traversal(rec_pair->record[1])) {
			bucket = (struct eh_bucket *)eh_record_bucket(rec_pair->record[1]);
			slot_id = eh_record_id(rec_pair->record[1]);
			depth = eh_record_depth(rec_pair->record[1]);
			layer = eh_record_layer(rec_pair->record[1]);
			new_slot = rec_pair->slot[1];
			goto append_eh_slot_next_segment;
		}
	}

	header = READ_ONCE(bucket->header);

	next_seg = retrieve_eh_high_segment(header);

	if (next_seg && !eh_bucket_stayed(header)) {
		if (unlikely(eh_bucket_defered(header))) {
			state = insert_eh_defered_bucket(&bucket[1], key, new_slot, f1, true);

			if (state == EXISTED_SLOT)
				return EXISTED_SLOT;

			if (state == SUCCESS_SLOT)
				return 0;
		}

		slot_id = 0;
		bucket_id = eh_seg2_bucket_idx(hashed_key, depth);
		bucket = &next_seg->bucket[bucket_id];
		depth += 1;
		layer += 1;

		f1 = hashed_key_fingerprint(hashed_key, depth, 18);
		new_slot = make_eh_ext2_slot(f1, kv);

		goto append_eh_slot_next_segment;
	}

	if (invalid && check_bucket_traversal(rec_pair->record[0]))
		goto retry_append_eh_slot;

	/*if (unlikely(defer)) {
		state = insert_eh_defered_bucket(&bucket[1], key, new_slot, f1, next_seg != NULL);

		if (state == SUCCESS_SLOT)
			return 0;

		if (state == EXISTED_SLOT)
			return EXISTED_SLOT;
	}*/

	record_bucket_initial_traversal(rec_pair);
	return LACK_SLOT;
}

__attribute__((always_inline))
static int append_eh_slot(
			struct eh_slot_record_pair *rec_pair, 
			struct kv *kv, 
			KEY_ITEM key, 
			u64 hashed_key) {
	EH_BUCKET_SLOT *slot;
	struct eh_bucket *bucket;
	int slot_id, layer;

	bucket = (struct eh_bucket *)eh_record_bucket(rec_pair->record[0]);
	slot_id = eh_record_id(rec_pair->record[0]);

	slot = &bucket->kv[slot_id];

	if (likely(cas_bool(slot, FREE_EH_SLOT, rec_pair->slot[0]))) {
		layer = eh_record_layer(rec_pair->record[0]);
		return EH_SLOT_NUM_PER_BUCKET * layer + slot_id;
	}

	return __append_eh_slot(rec_pair, kv, key, hashed_key);
}

static void __boost_eh_split_entry(
				struct eh_seg_context *seg_context, 
				struct eh_bucket *bucket,
				EH_BUCKET_HEADER bucket_header, 
				u64 hashed_key, 
				SPLIT_PRIORITY priority, 
				bool urgent) {
	EH_BUCKET_HEADER header, new_header, old_header;
	struct eh_segment *buttom_seg, *next_seg, *seg;
	struct eh_split_entry *s_ent, *old_s_ent;
	int depth, right;
	bool head, fast;
	SPLIT_PRIORITY old_priority;

	seg = seg_context->cur_seg;
	header = READ_ONCE(seg->bucket[0].header);

	depth = seg_context->depth;
	buttom_seg = seg_context->buttom_seg;
	fast = seg_context->fast;

	head = (bucket == &seg->bucket[0]);

re_boost_eh_split_entry :
	if (unlikely(header == INITIAL_EH_BUCKET_HEADER))
		return;

	if (header == INITIAL_EH_BUCKET_TOP_HEADER)
		goto finish_boost_eh_split_entry;

	if (eh_seg_low(header)) {
		right = eh_seg_id_in_seg2(hashed_key, depth - 1);
		next_seg = eh_next_high_seg(header);

		if (unlikely(!eh_four_seg(header)))
			return;

		/*if (eh_four_seg(header))
			next_seg += MUL_2(right, 1);
		else if (right == 1)
			return;*/
		next_seg += MUL_2(right, 1);

		/*header = set_eh_seg_low(next_seg);
		header = set_eh_bucket_stayed(header);*/
		fast = eh_fast_seg(header);
		header = set_eh_seg_low_ent(next_seg, true, fast, true, false, false, false);
		goto finish_boost_eh_split_entry;
	}

	old_s_ent = eh_split_entry_addr(header);
	old_priority = eh_split_entry_priority(header);

	if (unlikely(old_priority == DEFER_PRIO))
		return;

	if (unlikely(old_priority == THREAD_PRIO || old_priority == EMERGENCY_PRIO))
		goto finish_boost_eh_split_entry;

	if (priority == LOW_PRIO || priority == INCOMPLETE_PRIO ||
		(priority == HIGH_PRIO && old_priority == HIGH_PRIO)) {
		if (!urgent || eh_seg_urgent(header))
			goto finish_boost_eh_split_entry;
		
		s_ent = NULL; 

		new_header = set_eh_seg_urgent(header);
	} else {
		s_ent = new_split_record(priority, fast);

		if (unlikely(s_ent == (struct eh_split_entry *)INVALID_ADDR))
			return;

		new_header = set_eh_split_entry(s_ent, priority);

		if (urgent)
			new_header = set_eh_seg_urgent(new_header);
	}

	old_header = cas(&seg->bucket[0].header, header, new_header);

	if (unlikely(old_header != header)) {
	    if (s_ent)
		    invalidate_eh_split_entry(s_ent);

		header = old_header;
		goto re_boost_eh_split_entry;
	}

	header = new_header;

	if (s_ent) {
		if (unlikely(upgrade_eh_split_entry(old_s_ent, buttom_seg)))
			invalidate_eh_split_entry(s_ent);
		else {
			init_eh_split_entry(s_ent, buttom_seg, seg, hashed_key, depth - 1, NORMAL_SPLIT);

			memory_fence();
			header = READ_ONCE(seg->bucket[0].header);

			if (unlikely(new_header != header)) {
				modify_eh_split_entry(s_ent, header, buttom_seg, seg, hashed_key, depth - 1);
				return;
			}
		}
	}

finish_boost_eh_split_entry :
	if (!head)
		cas(&bucket->header, bucket_header, header);
}

__attribute__((always_inline))
static int boost_eh_split_entry(
				struct eh_seg_context *seg_context, 
				struct eh_bucket *bucket,
				EH_BUCKET_HEADER bucket_header, 
				u64 hashed_key, 
				int slot_id, 
				bool same_node) {
	bool urgent;
	SPLIT_PRIORITY priority;
	//SPLIT_STATE state;
	//head = (bucket == (struct eh_bucket *)&seg_context->cur_seg->bucket[0]);

	if (likely(slot_id < EH_SLOT_NUM_PER_BUCKET)) {
		if (same_node && slot_id >= EH_ADV_PREALLOCATE_THREHOLD1)
			hint_eh_seg_prefault(1);

		return 0;
	}

	if ((!same_node && slot_id < EH_EMERGENCY_SPLIT_THREHOLD) ||
			slot_id >= MUL_2(EH_SLOT_NUM_PER_BUCKET, 1) || 
			slot_id < seg_context->layer * EH_SLOT_NUM_PER_BUCKET)
		return 0;

	if (unlikely(bucket_header == INITIAL_EH_BUCKET_TOP_HEADER))
		return 0;

	priority = (bucket_header != INITIAL_EH_BUCKET_HEADER) ? 
				eh_split_entry_priority(bucket_header) : INCOMPLETE_PRIO;

	if (unlikely(priority == THREAD_PRIO || priority == DEFER_PRIO))
		return 0;

	if (slot_id >= EH_ADV_PREALLOCATE_THREHOLD2)
		hint_eh_seg_prefault(1);

	if (slot_id >= EH_EMERGENCY_SPLIT_THREHOLD) {
		if (same_node) {
			if (slot_id >= EH_URGENT_SPLIT_THREHOLD || is_prefault_seg_enough(4))
				return 1;

			if (priority == EMERGENCY_PRIO)
				return 0;

			priority = EMERGENCY_PRIO;
		} else {
			if (bucket_header != INITIAL_EH_BUCKET_HEADER && eh_seg_urgent(bucket_header))
				return 0;

			priority = INCOMPLETE_PRIO;
		}
		
		urgent = true;
	} else {
		if (slot_id < EH_BOOST_SPLIT_THREHOLD) {
			if (priority != INCOMPLETE_PRIO)
				return 0;

			priority = LOW_PRIO;
		} else {
			if (priority == HIGH_PRIO)
				return 0;

			priority = HIGH_PRIO;
		}
		
		urgent = false;
	}

	__boost_eh_split_entry(seg_context, bucket, bucket_header, hashed_key, priority, urgent);
	return 0;
}

__attribute__((always_inline))
static bool check_defer_bucket_candidate(
						struct eh_segment *seg, 
						struct eh_bucket *bucket) {
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot;

	if (bucket == &seg->bucket[EH_BUCKET_NUM - 1])
		return false;

	slot = READ_ONCE(bucket[1].kv[EH_SLOT_NUM_PER_BUCKET - 1]);
	acquire_fence();

	if (eh_slot_free(slot))
		return true;

	header = READ_ONCE(bucket->header);
	return eh_bucket_defered(header);
}

struct eh_segment *add_eh_new_segment_for_buttom(
						struct eh_seg_context *seg_context, 
						struct eh_bucket *bucket,
						EH_BUCKET_SLOT slot_val, 
						u64 hashed_key) {
	EH_BUCKET_HEADER header, new_header, old_header, next_header;
	struct eh_bucket *new_bucket;
	struct eh_split_entry *s_ent, *old_s_ent;
	struct eh_segment *seg, *next_seg;
	int bucket_id, depth, g_depth, nid;
	bool head, fast, stayed, defer, initial, same, no_defer, seg_enough;
	SEG_POSITION pos;
	SPLIT_PRIORITY priority;

	seg = seg_context->cur_seg;
	pos = seg_context->pos;
	depth = seg_context->depth;

	header = READ_ONCE(seg->bucket[0].header);

	head = (bucket == &seg->bucket[0]);
	stayed = !head || slot_val == FREE_EH_SLOT;

	next_seg = NULL;

	g_depth = get_eh_depth(hashed_key);

	if (unlikely(eh_seg_low(header)))
		goto add_eh_existed_buttom_segment;

	defer = eh_bucket_defered(header);
	initial = eh_bucket_initial(header) || (eh_split_entry_priority(header) != DEFER_PRIO);

	switch (pos) {
	case BUTTOM_SEG:
		seg_enough = is_prefault_seg_enough(2);

		if (seg_enough || g_depth <= depth ||
				!check_defer_bucket_candidate(seg, bucket)) {
			next_seg = alloc_eh_seg(2, &fast);
			
			if (fast && !is_tls_help_split())
				priority = THREAD_PRIO;
			else if (unlikely(!fast && seg_enough && g_depth > depth && 
								check_defer_bucket_candidate(seg, bucket))) {
				if ((void *)next_seg != INVALID_ADDR) {
					reclaim_page(next_seg, EH_SEGMENT_SIZE_BITS + 1);
					next_seg = NULL;
				}

				priority = DEFER_PRIO;
			} else
				priority = LOW_PRIO;

			no_defer = initial;
		} else
			priority = DEFER_PRIO;

		same = true;
		break;
	case DEFER_SEG:
		next_seg = alloc_eh_seg(2, &fast);

		if (unlikely(slot_val == FREE_EH_SLOT && !fast)) {
			if ((void *)next_seg != INVALID_ADDR)
				reclaim_page(next_seg, EH_SEGMENT_SIZE_BITS + 1);

			return NULL;
		}

		priority = HIGH_PRIO;//priority = LOW_PRIO;
		no_defer = true;
		same = true;
		break;
	case NUMA_BUTTOM_SEG:
		nid = seg_context->node_id;

		if (g_depth <= depth || 
					!check_defer_bucket_candidate(seg, bucket)) {
			next_seg = alloc_other_eh_seg(2, nid, &fast);
			priority = LOW_PRIO;
			no_defer = initial;
		} else
			priority = DEFER_PRIO;

		same = false;
		break;
	case NUMA_DEFER_SEG:
		nid = seg_context->node_id;
		next_seg = alloc_other_eh_seg(2, nid, &fast);
		priority = HIGH_PRIO;//priority = LOW_PRIO;
		no_defer = true;
		same = false;
		break;
	default:
		return (struct eh_segment *)INVALID_ADDR;
	}

	if (unlikely((void *)next_seg == INVALID_ADDR))
		return (slot_val != FREE_EH_SLOT) ? (struct eh_segment *)INVALID_ADDR : NULL;

	if (unlikely(g_depth <= depth)) {
		if (pos == BUTTOM_SEG || pos == NUMA_BUTTOM_SEG)
			no_defer = false;

		fast = false;
		priority = LOW_PRIO;
	}

	if (priority == DEFER_PRIO) {
		if (slot_val == FREE_EH_SLOT)
			return NULL;

		if (!initial)
			goto add_eh_defered_buttom_segment;
	}

//new_split_entry_for_buttom_new_segment :
	s_ent = same ? 
				new_split_record(priority, fast) : 
				new_other_split_record(priority, nid, fast);

	if (unlikely(s_ent == (struct eh_split_entry *)INVALID_ADDR)) {
		if (next_seg)
			reclaim_page(next_seg, EH_SEGMENT_SIZE_BITS + 1);

		return (slot_val != FREE_EH_SLOT) ? (struct eh_segment *)INVALID_ADDR : NULL;
	}

	if (priority != DEFER_PRIO) {
		if (priority == THREAD_PRIO)
			init_tls_split_entry(seg, next_seg, hashed_key, depth, NORMAL_SPLIT);

		new_header = set_eh_seg_low_ent(next_seg, false, fast, stayed, false, defer, !initial);

		bucket_id = eh_seg2_bucket_idx(hashed_key, depth);
		new_bucket = &next_seg->bucket[bucket_id];

		next_header = set_eh_split_entry(s_ent, priority);
		new_bucket->header = next_header;
		next_seg->bucket[0].header = next_header;

		if (slot_val != FREE_EH_SLOT && no_defer)
			new_bucket->kv[0] = slot_val;
	} else {
		/*if (header != INITIAL_EH_BUCKET_HEADER)
			goto add_eh_defered_buttom_segment;*/
		new_header = set_eh_split_entry(s_ent, DEFER_PRIO);

		if (head)
			new_header = set_eh_bucket_defered(new_header);

		init_eh_split_entry(s_ent, seg, NULL, hashed_key, depth, NORMAL_SPLIT);
	}

	while (1) {
		old_header = cas(&seg->bucket[0].header, header, new_header);

		if (likely(header == old_header)) {
			if (priority == DEFER_PRIO)
				return (struct eh_segment *)SHIFT(EH_BUCKET_DEFER_BIT);
			else {
				if (!initial) {
					old_s_ent = eh_split_entry_addr(header);
					invalidate_eh_split_entry(old_s_ent);
				}

				if (priority == THREAD_PRIO)
					prefetch_eh_segment_for_normal_split(seg, next_seg, 0);
				else {
					init_eh_split_entry(s_ent, seg, next_seg, hashed_key, depth, NORMAL_SPLIT);

					memory_fence();
					header = READ_ONCE(next_seg->bucket[0].header);

					if (unlikely(next_header != header))
						modify_eh_split_entry(s_ent, header, seg, next_seg, hashed_key, depth);
				}

				if (!no_defer) {
					seg_context->fast = fast;
					seg_context->buttom_seg = (struct eh_segment *)SHIFT(EH_BUCKET_DEFER_BIT);
					return next_seg;
				}
				/*if (!no_defer)
					return slot_val != FREE_EH_SLOT ? (struct eh_segment *)~SHIFT(EH_BUCKET_DEFER_BIT) : NULL;*/
			}

			seg_context->cur_seg = next_seg;
			seg_context->fast = fast;
			return next_seg;
		}

		header = old_header;

		if (eh_seg_low(header)) {
			invalidate_eh_split_entry(s_ent);

			if (next_seg)
				reclaim_page(next_seg, EH_SEGMENT_SIZE_BITS + 1);

			goto add_eh_existed_buttom_segment;
		}

		initial = eh_bucket_initial(header) || (eh_split_entry_priority(header) != DEFER_PRIO);

		if (priority == DEFER_PRIO) {
			if (!initial) {
				invalidate_eh_split_entry(s_ent);
				goto add_eh_defered_buttom_segment;
			}
			
			continue;
		}

		defer = eh_bucket_defered(header);

		if (defer)
			new_header = set_eh_bucket_defered(new_header);

		if (!initial) {
			if (no_defer && (pos == BUTTOM_SEG || pos == NUMA_BUTTOM_SEG)) {
				new_bucket->kv[0] = FREE_EH_SLOT;
				no_defer = false;

				/*if (priority == IDLE_PRIO) {
					invalidate_eh_split_entry(s_ent);
					priority = LOW_PRIO;
					goto new_split_entry_for_buttom_new_segment;
				}*/
			}

			new_header = set_eh_bucket_rechecked(new_header);
		}
	}

add_eh_existed_buttom_segment :
	next_seg = eh_next_high_seg(header);
	fast = eh_fast_seg(header);
	seg_context->fast = fast;

	if (pos == BUTTOM_SEG || pos == NUMA_BUTTOM_SEG) {
		if (!fast && !eh_four_seg(header) && 
					check_defer_bucket_candidate(seg, bucket))
			return slot_val != FREE_EH_SLOT ? (struct eh_segment *)SHIFT(EH_BUCKET_DEFER_BIT) : NULL;

		if (eh_bucket_rechecked(header)) {
			seg_context->buttom_seg = (struct eh_segment *)SHIFT(EH_BUCKET_DEFER_BIT);
			return next_seg;
		}
	} else if (!fast && slot_val == FREE_EH_SLOT)
		return NULL;

	if (!stayed) {
		new_header = cancel_eh_bucket_stayed(header);
		WRITE_ONCE(seg->bucket[0].header, new_header);
	}

	return next_seg;

add_eh_defered_buttom_segment :
	if (head) {
		new_header = set_eh_bucket_defered(header);
		cas(&seg->bucket[0].header, header, new_header);
	}

	return (struct eh_segment *)SHIFT(EH_BUCKET_DEFER_BIT);
}

struct eh_segment *add_eh_new_segment_for_top(
						struct eh_seg_context *seg_context, 
						struct eh_bucket *bucket,
						EH_BUCKET_SLOT slot_val, 
						u64 hashed_key) {
	EH_BUCKET_HEADER header, new_header, old_header, next_header;
	struct eh_bucket *new_bucket;
	struct eh_split_entry *s_ent, *old_s_ent;
	struct eh_segment *buttom_seg, *seg, *next_seg, *target_seg, *dest_seg;
	int bucket_id, depth, g_depth, nid, right, right2;
	bool head, fast, stayed;
	SEG_POSITION pos;
	SPLIT_PRIORITY priority, old_priority;

	seg = seg_context->cur_seg;
	pos = seg_context->pos;
	buttom_seg = seg_context->buttom_seg;
	depth = seg_context->depth - 1;

	header = READ_ONCE(seg->bucket[0].header);

	head = (bucket == &seg->bucket[0]);
	stayed = !head || slot_val == FREE_EH_SLOT;
	right = eh_seg_id_in_seg2(hashed_key, depth);

	g_depth = get_eh_depth(hashed_key);

	priority = URGENT_PRIO;

	if (unlikely(eh_seg_low(header)))
		goto add_eh_existed_top_segment;

	if (unlikely(header == INITIAL_EH_BUCKET_HEADER))
		goto add_eh_new_segment_for_top_to_buttom;
		
		
	if (header != INITIAL_EH_BUCKET_TOP_HEADER) {
		old_priority = eh_split_entry_priority(header);
		old_s_ent = eh_split_entry_addr(header);
		
		if (unlikely(old_priority == DEFER_PRIO))
			goto add_eh_new_segment_for_top_to_buttom;
			
		if (unlikely(old_priority == THREAD_PRIO && slot_val == FREE_EH_SLOT)) {
			cas(&bucket->header, INITIAL_EH_BUCKET_HEADER, header);
			return NULL;
		}
	}

	if (pos != NUMA_TOP_SEG) {
		next_seg = alloc_eh_seg(4, &fast);

		if (fast && !is_tls_help_split())
			priority = THREAD_PRIO;
	} else {
		nid = seg_context->node_id;
		next_seg = alloc_other_eh_seg(4, nid, &fast);
	}

	if (unlikely(g_depth <= depth)) {
		fast = false;
		priority = URGENT_PRIO;
	}

	if (unlikely((void *)next_seg == INVALID_ADDR))
		return (slot_val != FREE_EH_SLOT) ? (struct eh_segment *)INVALID_ADDR : NULL;

	new_header = set_eh_seg_low_ent(next_seg, true, fast, stayed, false, false, false);

	bucket_id = eh_seg4_bucket_idx(hashed_key, depth);
	new_bucket = &next_seg->bucket[bucket_id];

	if (header == INITIAL_EH_BUCKET_TOP_HEADER) {
		s_ent = NULL;
		priority = URGENT_PRIO;
		next_header = INITIAL_EH_BUCKET_TOP_HEADER;

		goto try_add_eh_new_segment_for_top;
	}
	
	if (unlikely(old_priority == THREAD2_PRIO || old_priority == THREAD_PRIO))
		priority = URGENT_PRIO;
	
new_split_entry_for_top_new_segment :
	s_ent = (pos != NUMA_TOP_SEG) ? 
				new_split_record(priority, fast) : 
				new_other_split_record(priority, nid, fast);

	if (unlikely((void *)s_ent == INVALID_ADDR)) {
		reclaim_page(next_seg, EH_SEGMENT_SIZE_BITS + 2);
		return (slot_val != FREE_EH_SLOT) ? (struct eh_segment *)INVALID_ADDR : NULL;
	}

retrieve_split_entry_for_top_new_segment :
	if (unlikely(old_priority == THREAD2_PRIO)) {
		depth -= 1;
		right2 = eh_seg_id_in_seg2(hashed_key, depth);

		target_seg = eh_atomic_split_target_seg(old_s_ent);
		dest_seg = seg - MUL_2(right2, 1);
	} else {
		target_seg = buttom_seg;
		dest_seg = next_seg;
	}

	if (priority == URGENT_PRIO)
		next_header = INITIAL_EH_BUCKET_TOP_HEADER;
	else {
		inform_tls_split_target(target_seg);
		next_header = set_eh_split_entry(s_ent, THREAD2_PRIO);
	}

try_add_eh_new_segment_for_top :
	next_seg->bucket[0].header = next_header;
	next_seg[2].bucket[0].header = next_header;

	if (slot_val != FREE_EH_SLOT)
		new_bucket->kv[0] = slot_val;

	while (1) {
		old_header = cas(&seg->bucket[0].header, header, new_header);

		if (likely(header == old_header)) {
			if (s_ent) {
				if (unlikely(old_priority == THREAD2_PRIO)) {
					seg = &dest_seg[MUL_2(right2 ^ 1, 1)];

					cas(&seg->bucket[0].header, 
							header, INITIAL_EH_BUCKET_TOP_HEADER);
				}
				
				if (unlikely(upgrade_eh_split_entry(old_s_ent, target_seg))) {
					invalidate_eh_split_entry(s_ent);
					
					if (priority == THREAD_PRIO) {
					    cas(&next_seg->bucket[0].header, next_header, INITIAL_EH_BUCKET_TOP_HEADER);
					    cas(&next_seg[2].bucket[0].header, next_header, INITIAL_EH_BUCKET_TOP_HEADER);
					}
				} else if (priority == THREAD_PRIO) {
					init_tls_split_entry(target_seg, dest_seg, hashed_key, depth, URGENT_SPLIT);
					
					memory_fence();
					header = READ_ONCE(dest_seg->bucket[0].header);
					
					if (likely(header == next_header))
						prefetch_eh_segment_for_urgent_split(target_seg, seg, dest_seg, 0);
					else {
						cas(&dest_seg[2].bucket[0].header, next_header, INITIAL_EH_BUCKET_TOP_HEADER);
						
						modify_tls_split_entry(target_seg, dest_seg);
					}
				} else
					init_eh_split_entry(s_ent, target_seg, dest_seg, hashed_key, depth, URGENT_SPLIT);
			}

			next_seg += MUL_2(right, 1);

			seg_context->cur_seg = next_seg;
			seg_context->fast = fast;
			return next_seg;
		}

		header = old_header;

		if (eh_seg_low(header)) {
			if (s_ent)
				invalidate_eh_split_entry(s_ent);

			reclaim_page(next_seg, EH_SEGMENT_SIZE_BITS + 2);
			goto add_eh_existed_top_segment;
		}

		if (unlikely(s_ent != NULL && old_priority == THREAD2_PRIO)) {
			depth += 1;
			target_seg = buttom_seg;
			dest_seg = next_seg;
		}

		if (!eh_bucket_initial(header)) {
			old_s_ent = eh_split_entry_addr(header);
			old_priority = eh_split_entry_priority(header);
			
			if (unlikely(old_priority == DEFER_PRIO)) {
				if (s_ent)
					invalidate_eh_split_entry(s_ent);
					
				reclaim_page(next_seg, EH_SEGMENT_SIZE_BITS + 2);
				goto add_eh_new_segment_for_top_to_buttom;
			}
			
			if (s_ent == NULL)
				goto new_split_entry_for_top_new_segment;

			goto retrieve_split_entry_for_top_new_segment;
		} else {
			if (s_ent) {
				invalidate_eh_split_entry(s_ent);
				s_ent = NULL;

				next_seg->bucket[0].header = INITIAL_EH_BUCKET_TOP_HEADER;
				next_seg[2].bucket[0].header = INITIAL_EH_BUCKET_TOP_HEADER;
			}

			if (header == INITIAL_EH_BUCKET_HEADER) {
				reclaim_page(next_seg, EH_SEGMENT_SIZE_BITS + 2);
				goto add_eh_new_segment_for_top_to_buttom;
			}
		}
	}

add_eh_existed_top_segment :
	next_seg = eh_next_high_seg(header);
	seg_context->fast = eh_fast_seg(header);

	if (eh_four_seg(header)) {
		if (!stayed) {
			new_header = cancel_eh_bucket_stayed(header);
			WRITE_ONCE(seg->bucket[0].header, new_header);
		}

		return (next_seg + MUL_2(right, 1));
	}
	/*if (right == 0)
		return next_seg;*/

add_eh_new_segment_for_top_to_buttom :
	if (slot_val != FREE_EH_SLOT) {
		seg += right;

		seg_context->pos = (pos != NUMA_TOP_SEG) ? BUTTOM_SEG : NUMA_BUTTOM_SEG;
		seg_context->cur_seg = seg;
		seg_context->layer = 0;
		//seg_context->fast = true;
	}

	return NULL;
}

struct eh_segment *add_eh_new_segment(
						struct eh_seg_context *seg_context, 
						struct eh_segment *next_seg, 
						struct eh_bucket *bucket,
						EH_BUCKET_HEADER bucket_header, 
						u64 hashed_key, 
						struct kv *kv) {
	EH_BUCKET_HEADER new_header;
	EH_BUCKET_SLOT slot_val;
	u64 fingerprint;
	struct eh_segment *ret_seg, *seg;
	struct eh_bucket *new_bucket;
	int depth, bucket_id;
	bool head, defer, four;
	SEG_POSITION pos;

	depth = seg_context->depth;
	pos = seg_context->pos;

	if (kv) {
		fingerprint = hashed_key_fingerprint(hashed_key, depth + 1, 18);
		slot_val = make_eh_ext2_slot(fingerprint, kv);
	} else 
		slot_val = FREE_EH_SLOT;

	if (next_seg) {
		new_header = cancel_eh_bucket_stayed(bucket_header);
		head = false;
	} else {
		if (pos == TOP_SEG || pos == NUMA_TOP_SEG) {
			seg = seg_context->cur_seg;
			head = (bucket == &seg->bucket[0]);

			next_seg = add_eh_new_segment_for_top(seg_context, bucket, 
													slot_val, hashed_key);
			pos = seg_context->pos;
			defer = false;
			four = true;
		}

		if (pos != TOP_SEG && pos != NUMA_TOP_SEG) {
			seg = seg_context->cur_seg;
			head = (bucket == &seg->bucket[0]);
			
			next_seg = add_eh_new_segment_for_buttom(seg_context, 
										bucket, slot_val, hashed_key);

			if (next_seg == (struct eh_segment *)SHIFT(EH_BUCKET_DEFER_BIT)) {
				ret_seg = (struct eh_segment *)SHIFT(EH_BUCKET_DEFER_BIT);

				new_header = set_eh_bucket_defered(bucket_header);
				kv = NULL;
				goto finish_add_eh_new_segment;
			}

			defer = eh_bucket_defered(bucket_header);
			four = false;
		}

		if (unlikely(next_seg == (struct eh_segment *)INVALID_ADDR))
			return (struct eh_segment *)INVALID_ADDR;

		if (next_seg == NULL)
			return NULL;

		//new_header = set_eh_seg_low(next_seg);
		new_header = set_eh_seg_low_ent(next_seg, four, seg_context->fast, kv == NULL, false, defer, false);

		if (next_seg == seg_context->cur_seg) {
			ret_seg = NULL;
			goto finish_add_eh_new_segment;
		}

		if (unlikely(seg_context->buttom_seg == (struct eh_segment *)SHIFT(EH_BUCKET_DEFER_BIT))) {
			seg_context->buttom_seg = NULL;

			if (head || unlikely(!cas_bool(&bucket->header, bucket_header, new_header)))
				return kv == NULL ? NULL : (struct eh_segment *)SHIFT(EH_BUCKET_DEFER_BIT);

			head = true;
		}
	}

	if (kv == NULL)
		ret_seg = NULL;
	else {
		bucket_id = eh_seg2_bucket_idx(hashed_key, depth);
		new_bucket = &next_seg->bucket[bucket_id];

		if (!cas_bool(&new_bucket->kv[0], FREE_EH_SLOT, slot_val))
			ret_seg = next_seg;
		else {
			ret_seg = NULL;

			/*if (pos == BUTTOM_SEG && !head) {
			    seg_context->buttom_seg = seg_context->cur_seg;
				seg_context->cur_seg = next_seg;
				seg_context->depth = depth + 1;

				__boost_eh_split_entry(seg_context, new_bucket, 
							INITIAL_EH_BUCKET_HEADER, hashed_key, LOW_PRIO, false);
			}*/
		}
	}

finish_add_eh_new_segment :
	if (!head) {
	        //cas(&bucket->header, bucket_header, new_header);
		if (kv == NULL)
			cas(&bucket->header, bucket_header, new_header);
		else {
			release_fence();
			WRITE_ONCE(bucket->header, new_header);
		}
	}

	return ret_seg;
}


__attribute__((always_inline)) 
static void eh_help_split() {
	struct eh_split_context *s_context; 
	int ret;
	
	s_context = possess_tls_split_context();

	if (s_context == NULL) {
	    if (get_epoch_per_thread() % 16 == 0)
		    retrieve_tls_help_split();
		
		return;
	}

	ret = eh_split(s_context);

	if (ret == 0)
		invalidate_eh_split_entry(&s_context->entry);
	else if (ret == 1) {
		ret = dispossess_tls_split_context();

		if (unlikely(ret == -1)) {
			//to doooooooooo handle memory error and redo upgrade tls split entry
		}
	} else {
		//to dooooooooooooo handle memory error and redo memory fail handle logging
	}
}

int insert_eh_seg_kv(
			struct kv *kv, 
			KEY_ITEM key, 
			u64 hashed_key, 
			struct eh_bucket *bucket, 
			struct eh_seg_context *seg_context) {
	struct eh_segment *next_seg;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot, new_slot;
	struct eh_slot_record_pair rec_pair;
	u64 fingerprint;
	enum eh_slot_state state;
	int bucket_id, slot_id;
	bool invalid, stay, defer, same_node;

	same_node = (seg_context->node_id == tls_node_id());
	seg_context->pos = (same_node ? BUTTOM_SEG : NUMA_BUTTOM_SEG);
	seg_context->layer = 0;
	
	record_bucket_initial_traversal(&rec_pair);
	
	eh_help_split();

insert_eh_next_seg_kv :
	fingerprint = hashed_key_fingerprint(hashed_key, seg_context->depth, 18);
	new_slot = make_eh_ext2_slot(fingerprint, kv);
	fingerprint = fingerprint >> 2;

	bucket_id = eh_seg2_bucket_idx(hashed_key, seg_context->depth);

	header = READ_ONCE(bucket->header);

	defer = eh_bucket_defered(header);

	if (unlikely(defer))
		prefetch_eh_defered_bucket(&bucket[1]);

	next_seg = retrieve_eh_high_segment(header);

	if (unlikely(next_seg)) {
		seg_context->fast = eh_fast_seg(header);

		if (unlikely(eh_seg_splited(header))) {
			defer = false;
			goto insert_eh_next_seg_kv_ready;
		}

		stay = eh_bucket_stayed(header);

		if (!stay)
			prefetch_eh_bucket_head(&next_seg->bucket[bucket_id]);
	}

	invalid = false;

	for (slot_id = 0; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
		slot = READ_ONCE(bucket->kv[slot_id]);

		if (unlikely(eh_slot_end(slot)))
			break;

		if (eh_slot_free(slot)) {
			record_bucket_traversal(&rec_pair, new_slot, bucket, slot_id, 
								seg_context->depth, seg_context->layer);
			break;
		}

		if (fingerprint == eh_slot_fingerprint(slot, 16)) {
			state = insert_eh_matched_slot(key, slot, &bucket->kv[slot_id], new_slot);

			if (state == EXISTED_SLOT)
				return 1;

			if (state == SUCCESS_SLOT)
				return 0;

			if (unlikely(state == INVALID_SLOT))
				invalid = true;
		}
	}

	acquire_fence();

	while (!next_seg || unlikely(stay)) {
		if (unlikely(invalid)) {
			header = READ_ONCE(bucket->header);
			next_seg = retrieve_eh_high_segment(header);
			
			defer = eh_bucket_defered(header);
			
			if (next_seg) {
				stay = eh_bucket_stayed(header);
				seg_context->fast = eh_fast_seg(header);
				
				if (!stay)
					goto insert_eh_next_seg_kv_ready;
			}
			
			invalid = false;
		} else if (check_bucket_traversal(rec_pair.record[0])) {
			slot_id = append_eh_slot(&rec_pair, kv, key, hashed_key);

			if (likely(slot_id >= 0)) {
				if (next_seg || boost_eh_split_entry(seg_context, bucket, 
										header, hashed_key, slot_id, same_node) == 0) {
					if (!is_prefault_seg_enough(DIV_2(MAX_EH_PREFETCH_SEG, 2))) 
						eh_help_split();
					
					return 0;
				}

				kv = NULL;
			} else if (slot_id == EXISTED_SLOT)
				return 1;
		}

		if (defer) {
			state = insert_eh_defered_bucket(&bucket[1], key, new_slot, fingerprint, next_seg != NULL);

			if (state == SUCCESS_SLOT) {
				if (!same_node || !is_prefault_seg_enough(2))
					return 0;

				kv = NULL;
			} else if (state == EXISTED_SLOT)
				return 1;

			seg_context->pos = (same_node ? DEFER_SEG : NUMA_DEFER_SEG);

			if (unlikely(seg_context->layer != 0)) {
				seg_context->layer = 0;
				seg_context->cur_seg += eh_seg_id_in_seg2(hashed_key, seg_context->depth - 1);
			}
		}

		next_seg = add_eh_new_segment(seg_context, next_seg, bucket, header, hashed_key, kv);

		if (likely(next_seg == NULL))
			return 0;

		if (unlikely((void *)next_seg == INVALID_ADDR))
			return -1;

		if (next_seg == (struct eh_segment *)SHIFT(EH_BUCKET_DEFER_BIT)) {
			header = READ_ONCE(bucket->header);

			defer = eh_bucket_defered(header);
			next_seg = retrieve_eh_high_segment(header);

			if (next_seg) {
				seg_context->fast = eh_fast_seg(header);
				stay = eh_bucket_stayed(header);
			}

			continue;
		}

		defer = false;
		break;
	}

insert_eh_next_seg_kv_ready :
	if (unlikely(defer)) {
		state = insert_eh_defered_bucket(&bucket[1], key, new_slot, fingerprint, true);

		if (state == SUCCESS_SLOT)
			return 0;

		if (state == EXISTED_SLOT)
			return 1;
	}

	seg_context->buttom_seg = seg_context->cur_seg;

	if (seg_context->layer != 0)
		seg_context->buttom_seg += eh_seg_id_in_seg2(hashed_key, seg_context->depth - 1);

	seg_context->layer += 1;
	seg_context->depth += 1;
	seg_context->pos = (same_node ? TOP_SEG : NUMA_TOP_SEG);
	seg_context->cur_seg = next_seg;

	bucket = &next_seg->bucket[bucket_id];
	goto insert_eh_next_seg_kv;
}

int update_eh_seg_kv(
		struct kv *kv, 
		KEY_ITEM key, 
		u64 hashed_key, 
		struct eh_bucket *bucket, 
		int l_depth) {
	struct eh_segment *next_seg;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot, new_slot;
	struct eh_slot_record_pair rec_pair;
	u64 fingerprint;
	enum eh_slot_state state;
	int bucket_id, slot_id;
	bool invalid, stay, defer;

	//to dooooooooooooooooooooooo add extra function
	
	eh_help_split();

	record_bucket_initial_traversal(&rec_pair);

update_eh_next_seg_kv :
	fingerprint = hashed_key_fingerprint(hashed_key, l_depth, 18);
	new_slot = make_eh_ext2_slot(fingerprint, kv);
	fingerprint = fingerprint >> 2;

	bucket_id = eh_seg2_bucket_idx(hashed_key, l_depth);

	header = READ_ONCE(bucket->header);
	next_seg = retrieve_eh_high_segment(header);

	defer = eh_bucket_defered(header);

	if (unlikely(defer))
		prefetch_eh_defered_bucket(&bucket[1]);

	if (next_seg) {
		if (unlikely(eh_seg_splited(header)))
			goto update_eh_next_seg_kv_ready;

		stay = eh_bucket_stayed(header);

		if (!stay)
			prefetch_eh_bucket_head(&next_seg->bucket[bucket_id]);
	}

	invalid = false;

	for (slot_id = 0; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
		slot = READ_ONCE(bucket->kv[slot_id]);

		if (unlikely(eh_slot_end(slot)))
			break;

		if (eh_slot_free(slot)) {
			record_bucket_traversal(&rec_pair, new_slot, 
										bucket, slot_id, l_depth, 0);
			break;
		}

		if (fingerprint == eh_slot_fingerprint(slot, 16)) {
			state = update_eh_matched_slot(kv, key, hashed_key, slot, 
								&bucket->kv[slot_id], new_slot, &rec_pair);

			if (state == SUCCESS_SLOT)
				return 0;

			if (state == DELETED_SLOT)
				return 1;

			if (unlikely(state == INVALID_SLOT))
				invalid = true;
		}
	}

	if (unlikely(defer)) {
		state = update_eh_defered_bucket(&bucket[1], key, new_slot, fingerprint);

		if (state == SUCCESS_SLOT)
			return 0;

		if (state == DELETED_SLOT)
			return 1;

		if (unlikely(state == INVALID_SLOT))
			invalid = true;
	}

	acquire_fence();

	if (!next_seg || unlikely(stay)) {
		if (likely(!invalid))
			return 1;

		header = READ_ONCE(bucket->header);
		next_seg = retrieve_eh_high_segment(header);
		
		if (!next_seg)
			return 1;
		
		defer = eh_bucket_defered(header);
		        
		if (defer) {
			state = update_eh_defered_bucket(&bucket[1], key, new_slot, fingerprint);
			
			if (state == SUCCESS_SLOT)
				return 0;
				
			if (state == DELETED_SLOT)
				return 1;
		}
	}

update_eh_next_seg_kv_ready :
	l_depth += 1;
	bucket = &next_seg->bucket[bucket_id];
	goto update_eh_next_seg_kv;
}


int delete_eh_seg_kv(
		KEY_ITEM key, 
		u64 hashed_key, 
		struct eh_bucket *bucket, 
		int l_depth) {
	struct eh_segment *next_seg;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot;
	struct eh_slot_record_pair rec_pair;
	u64 fingerprint;
	enum eh_slot_state state;
	int bucket_id, slot_id;
	bool invalid, stay, defer;

	//to dooooooooooooooooooooooo add extra function
	eh_help_split();

	record_bucket_initial_traversal(&rec_pair);

delete_eh_next_seg_kv :
	fingerprint = hashed_key_fingerprint(hashed_key, l_depth, 16);

	bucket_id = eh_seg2_bucket_idx(hashed_key, l_depth);

	header = READ_ONCE(bucket->header);
	next_seg = retrieve_eh_high_segment(header);

	defer = eh_bucket_defered(header);

	if (unlikely(defer))
		prefetch_eh_defered_bucket(&bucket[1]);

	if (next_seg) {
		if (unlikely(eh_seg_splited(header)))
			goto delete_eh_next_seg_kv_ready;

		stay = eh_bucket_stayed(header);

		if (!stay)
			prefetch_eh_bucket_head(&next_seg->bucket[bucket_id]);
	}

	invalid = false;

	for (slot_id = 0; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
		slot = READ_ONCE(bucket->kv[slot_id]);

		if (unlikely(eh_slot_end(slot)))
			break;

		if (eh_slot_free(slot)) {
			record_bucket_traversal(&rec_pair, FREE_EH_SLOT, 
										bucket, slot_id, l_depth, 0);
			break;
		}

		if (fingerprint == eh_slot_fingerprint(slot, 16)) {
			state = delete_eh_matched_slot(key, hashed_key, slot, &bucket->kv[slot_id], &rec_pair);

			if (state == SUCCESS_SLOT)
				return 0;

			if (state == DELETED_SLOT)
				return 1;

			if (unlikely(state == INVALID_SLOT))
				invalid = true;
		}
	}

	if (unlikely(defer)) {
		state = delete_eh_defered_bucket(&bucket[1], key, fingerprint);

		if (state == SUCCESS_SLOT)
			return 0;

		if (state == DELETED_SLOT)
			return 1;

		if (unlikely(state == INVALID_SLOT))
			invalid = true;
	}

	acquire_fence();

	if (!next_seg || unlikely(stay)) {
		if (likely(!invalid))
			return 1;

		header = READ_ONCE(bucket->header);
		next_seg = retrieve_eh_high_segment(header);
		
		if (!next_seg)
			return 1;
		        
		defer = eh_bucket_defered(header);
		        
		if (defer) {
			state = delete_eh_defered_bucket(&bucket[1], key, fingerprint);
			
			if (state == SUCCESS_SLOT)
				return 0;
				
			if (state == DELETED_SLOT)
				return 1;
		}
	}

delete_eh_next_seg_kv_ready :
	l_depth += 1;
	bucket = &next_seg->bucket[bucket_id];
	goto delete_eh_next_seg_kv;
}

struct kv *lookup_eh_seg_kv(
		KEY_ITEM key,  
		u64 hashed_key, 
		struct eh_bucket *bucket,  
		int l_depth) {
	struct eh_segment *next_seg;
	struct kv *ret_kv;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot;
	u64 fingerprint;
	enum eh_slot_state state;
	int bucket_id, slot_id;
	bool stay, invalid, defer;

	eh_help_split();

lookup_eh_next_seg_kv :
	fingerprint = hashed_key_fingerprint(hashed_key, l_depth, 16);

	bucket_id = eh_seg2_bucket_idx(hashed_key, l_depth);

	header = READ_ONCE(bucket->header);
	next_seg = retrieve_eh_high_segment(header);

	defer = eh_bucket_defered(header);

	if (unlikely(defer))
		prefetch_eh_defered_bucket(&bucket[1]);

	if (next_seg) {
		if (unlikely(eh_seg_splited(header)))
			goto lookup_eh_next_seg_kv_ready;

		stay = eh_bucket_stayed(header);

		if (!stay)
			prefetch_eh_bucket_head(&next_seg->bucket[bucket_id]);
	}

	invalid = false;

	for (slot_id = 0; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
		slot = READ_ONCE(bucket->kv[slot_id]);

		if (eh_slot_free(slot) || unlikely(eh_slot_end(slot)))
			break;

		if (fingerprint == eh_slot_fingerprint(slot, 16)) {
			state = lookup_eh_matched_slot(key, &ret_kv, slot);

			if (state == EXISTED_SLOT)
				return ret_kv;

			if (state == DELETED_SLOT)
				return NULL;

			if (unlikely(state == INVALID_SLOT))
				invalid = true;
		}
	}

	if (unlikely(defer)) {
		state = lookup_eh_defered_bucket(&bucket[1], key, &ret_kv, fingerprint);

		if (state == EXISTED_SLOT)
			return ret_kv;

		if (state == DELETED_SLOT)
			return NULL;

		if (unlikely(state == INVALID_SLOT))
			invalid = true;
	}

	acquire_fence();

	if (!next_seg || unlikely(stay)) {
		if (likely(!invalid))
			return NULL;

		header = READ_ONCE(bucket->header);
		next_seg = retrieve_eh_high_segment(header);
		
		if (!next_seg)
			return NULL;
		        
		defer = eh_bucket_defered(header);
		        
		if (defer) {
			state = lookup_eh_defered_bucket(&bucket[1], key, &ret_kv, fingerprint);
			
			if (state == EXISTED_SLOT)
				return ret_kv;
				
			if (state == DELETED_SLOT)
				return NULL;
		}
	}

lookup_eh_next_seg_kv_ready :
	l_depth += 1;
	bucket = &next_seg->bucket[bucket_id];
	goto lookup_eh_next_seg_kv;
}


int update_eh_seg_kv_for_gc(
		struct kv *old_kv, 
		struct kv *new_kv, 
		u64 hashed_key, 
		struct eh_bucket *bucket, 
		int l_depth) {
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot, new_slot;
	u64 fingerprint;
	enum eh_slot_state state;
	int slot_id;

	fingerprint = hashed_key_fingerprint(hashed_key, l_depth, 18);
	new_slot = make_eh_ext2_slot(fingerprint, new_kv);
	fingerprint = fingerprint >> 2;

	header = READ_ONCE(bucket->header);

	if (unlikely(eh_seg_splited(header)))
		return 1;

	for (slot_id = 0; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
		slot = READ_ONCE(bucket->kv[slot_id]);

		if (unlikely(eh_slot_end(slot)))
			break;

		if (eh_slot_free(slot))
			return 1;

		if (fingerprint == eh_slot_fingerprint(slot, 16)) {
			if (likely(!eh_slot_invalid(slot)) && old_kv == eh_slot_kv_addr(slot)) {
				if (eh_slot_deleted(slot))
					new_slot = set_eh_slot_deleted(new_slot);
				
				if (likely(cas_bool(&bucket->kv[slot_id], slot, new_slot)))
					return 0;

				return 1;
			}
		}
	}

	return 1;
}
