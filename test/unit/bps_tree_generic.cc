#include <stdint.h>
#include <stdio.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>

#ifdef BPS_BLOCK_CHILD_POWER_ARRAY
#define UNIT_TAP_COMPATIBLE 1
#endif

#include "unit.h"
#include "sptree.h"
#include "qsort_arg.h"

#ifndef MAX
#define MAX(a,b) (((a)>(b))?(a):(b))
#endif //#ifndef MAX

SPTREE_DEF(test, realloc, qsort_arg);

typedef int64_t type_t;
#define TYPE_F "%" PRId64

static int
compare(type_t a, type_t b);

/* check compiling with another name and settings */
#define BPS_TREE_NAME testtest
#define BPS_TREE_BLOCK_SIZE 512
#define BPS_TREE_EXTENT_SIZE 16*1024
#define BPS_TREE_IS_IDENTICAL(a, b) (a == b)
#define BPS_TREE_COMPARE(a, b, arg) compare(a, b)
#define BPS_TREE_COMPARE_KEY(a, b, arg) compare(a, b)
#define bps_tree_elem_t char
#define bps_tree_key_t char
#define bps_tree_arg_t int
#include "salad/bps_tree.h"
#undef BPS_TREE_NAME
#undef BPS_TREE_BLOCK_SIZE
#undef BPS_TREE_EXTENT_SIZE
#undef BPS_TREE_IS_IDENTICAL
#undef BPS_TREE_COMPARE
#undef BPS_TREE_COMPARE_KEY
#undef bps_tree_elem_t
#undef bps_tree_key_t
#undef bps_tree_arg_t

/* true tree with true settings */
#define BPS_TREE_NAME test
#define BPS_TREE_BLOCK_SIZE SMALL_BLOCK_SIZE /* small specially for tests */
#define BPS_TREE_EXTENT_SIZE 2048 /* value is to low specially for tests */
#define BPS_TREE_IS_IDENTICAL(a, b) (a == b)
#define BPS_TREE_COMPARE(a, b, arg) compare(a, b)
#define BPS_TREE_COMPARE_KEY(a, b, arg) compare(a, b)
#define bps_tree_elem_t type_t
#define bps_tree_key_t type_t
#define bps_tree_arg_t int
#define BPS_TREE_DEBUG_BRANCH_VISIT
#include "salad/bps_tree.h"
#undef BPS_TREE_NAME
#undef BPS_TREE_BLOCK_SIZE
#undef BPS_TREE_EXTENT_SIZE
#undef BPS_TREE_IS_IDENTICAL
#undef BPS_TREE_COMPARE
#undef BPS_TREE_COMPARE_KEY
#undef bps_tree_elem_t
#undef bps_tree_key_t
#undef bps_tree_arg_t

struct elem_t {
	long info;
	long marker;
};

static bool
equal(const elem_t &a, const elem_t &b)
{
	return a.info == b.info && a.marker == b.marker;
}

static int compare(const elem_t &a, const elem_t &b)
{
	return a.info < b.info ? -1 : a.info > b.info ? 1 : 0;
}

static int compare_key(const elem_t &a, long b)
{
	return a.info < b ? -1 : a.info > b ? 1 : 0;
}

#define BPS_TREE_NAME struct_tree
#define BPS_TREE_BLOCK_SIZE SMALL_BLOCK_SIZE /* small specially for tests */
#define BPS_TREE_EXTENT_SIZE 2048 /* value is to low specially for tests */
#define BPS_TREE_IS_IDENTICAL(a, b) equal(a, b)
#define BPS_TREE_COMPARE(a, b, arg) compare(a, b)
#define BPS_TREE_COMPARE_KEY(a, b, arg) compare_key(a, b)
#define bps_tree_elem_t struct elem_t
#define bps_tree_key_t long
#define bps_tree_arg_t int
#include "salad/bps_tree.h"
#undef BPS_TREE_NAME
#undef BPS_TREE_BLOCK_SIZE
#undef BPS_TREE_EXTENT_SIZE
#undef BPS_TREE_COMPARE
#undef BPS_TREE_COMPARE_KEY
#undef BPS_TREE_IS_IDENTICAL
#undef bps_tree_elem_t
#undef bps_tree_key_t
#undef bps_tree_arg_t

/* tree for approximate_count test */
#define BPS_TREE_NAME approx
#define BPS_TREE_BLOCK_SIZE SMALL_BLOCK_SIZE /* small specially for tests */
#define BPS_TREE_EXTENT_SIZE 2048 /* value is to low specially for tests */
#define BPS_TREE_IS_IDENTICAL(a, b) (a == b)
#define BPS_TREE_COMPARE(a, b, arg) ((a) < (b) ? -1 : (a) > (b) ? 1 : 0)
#define BPS_TREE_COMPARE_KEY(a, b, arg) (((a) >> 32) < (b) ? -1 : ((a) >> 32) > (b) ? 1 : 0)
#define bps_tree_elem_t uint64_t
#define bps_tree_key_t uint32_t
#define bps_tree_arg_t int
#include "salad/bps_tree.h"

#define bps_insert_and_check(tree_name, tree, elem, replaced) \
{\
	tree_name##_iterator iter;\
	if (tree_name##_insert_get_iterator((tree), (elem), \
					    (replaced), &iter) == 0) {\
		type_t check_value =\
			*tree_name##_iterator_get_elem((tree), &iter);\
		if (check_value != (type_t)(elem)) {\
			printf("iterator doesn't point to the inserted "\
			       "element: %lld != %lld", (long long) (elem),\
			       (long long) check_value);\
			fail("elem != check_value", "true");\
		}\
	}\
}

static int
node_comp(const void *p1, const void *p2, void* unused)
{
	(void)unused;
	return *((const type_t *)p1) < *((const type_t *)p2) ? -1 : *((const type_t *)p2) < *((const type_t *)p1) ? 1 : 0;
}

static int
compare(type_t a, type_t b)
{
	return a < b ? -1 : a > b ? 1 : 0;
}
static int extents_count = 0;

static void *
extent_alloc(void *ctx)
{
	int *p_extents_count = (int *)ctx;
	assert(p_extents_count == &extents_count);
	++*p_extents_count;
	return malloc(BPS_TREE_EXTENT_SIZE);
}

static void
extent_free(void *ctx, void *extent)
{
	int *p_extents_count = (int *)ctx;
	assert(p_extents_count == &extents_count);
	--*p_extents_count;
	free(extent);
}

#define INSERT_AND_CHECK(tree, v) do { \
	fail_unless(test_find(tree, v) == NULL); \
	test_insert(tree, v, 0, 0); \
	fail_unless(test_find(tree, v) != NULL); \
	if (test_debug_check(tree)) { \
		test_print(tree, TYPE_F); \
		fail("debug check nonzero", "true"); \
	} \
} while (false)

#define DELETE_AND_CHECK(tree, v) do { \
	fail_unless(test_find(tree, v) != NULL); \
	test_delete(tree, v); \
	fail_unless(test_find(tree, v) == NULL); \
	if (test_debug_check(tree)) { \
		test_print(tree, TYPE_F); \
		fail("debug check nonzero", "true"); \
	} \
} while (false)

static void
simple_check()
{
	header();

	struct matras_stats stats;
	matras_stats_create(&stats);
	stats.extent_count = extents_count;

	const int rounds = 2000;
	test tree;
	test_create(&tree, 0, extent_alloc, extent_free, &extents_count,
		    &stats);

	printf("Insert 1..X, remove 1..X\n");

	for (int i = 0; i < rounds; i++)
		INSERT_AND_CHECK(&tree, i);
	fail_unless(test_size(&tree) == (size_t)rounds);
	fail_unless((int)stats.extent_count == extents_count);

	for (int i = 0; i < rounds; i++)
		DELETE_AND_CHECK(&tree, i);
	fail_unless(test_size(&tree) == 0);
	fail_unless((int)stats.extent_count == extents_count);

	printf("Insert 1..X, remove X..1\n");

	for (int i = 0; i < rounds; i++)
		INSERT_AND_CHECK(&tree, i);
	fail_unless(test_size(&tree) == (size_t)rounds);
	fail_unless((int)stats.extent_count == extents_count);

	for (int i = rounds - 1; i >= 0; i--)
		DELETE_AND_CHECK(&tree, i);
	fail_unless(test_size(&tree) == 0);
	fail_unless((int)stats.extent_count == extents_count);

	printf("Insert X..1, remove 1..X\n");

	for (int i = rounds - 1; i >= 0; i--)
		INSERT_AND_CHECK(&tree, i);
	fail_unless(test_size(&tree) == (size_t)rounds);
	fail_unless((int)stats.extent_count == extents_count);

	for (int i = 0; i < rounds; i++)
		DELETE_AND_CHECK(&tree, i);
	fail_unless(test_size(&tree) == 0);
	fail_unless((int)stats.extent_count == extents_count);

	printf("Insert X..1, remove X..1\n");

	for (int i = rounds - 1; i >= 0; i--)
		INSERT_AND_CHECK(&tree, i);
	fail_unless(test_size(&tree) == (size_t)rounds);
	fail_unless((int)stats.extent_count == extents_count);

	for (int i = rounds - 1; i >= 0; i--)
		DELETE_AND_CHECK(&tree, i);
	fail_unless(test_size(&tree) == 0);
	fail_unless((int)stats.extent_count == extents_count);

	test_destroy(&tree);

	footer();
}

#ifdef BPS_BLOCK_CHILD_POWER_ARRAY

#define INSERT_AND_CHECK_GET_POS(tree, v, expected_pos) do { \
	size_t pos; \
	fail_unless(test_find(tree, v) == NULL); \
	fail_unless(test_insert_get_offset(tree, v, 0, &pos) == 0); \
	fail_unless(pos == expected_pos); \
	type_t *found = test_find(tree, v); \
	fail_unless(found != NULL); \
	fail_unless(test_find_get_offset(tree, v, &pos) == found); \
	fail_unless(pos == expected_pos); \
	fail_unless(test_find_by_offset(tree, pos) == found); \
	if (test_debug_check(tree)) { \
		test_print(tree, TYPE_F); \
		fail("debug check nonzero", "true"); \
	} \
} while (false)

#define DELETE_AND_CHECK_GET_POS(tree, v, expected_pos) do { \
	size_t pos = SIZE_MAX; \
	fail_unless(test_find(tree, v) != NULL); \
	fail_unless(test_delete_get_offset(tree, v, &pos) == 0); \
	fail_unless(pos == expected_pos); \
	fail_unless(test_find(tree, v) == NULL); \
	if (test_debug_check(tree)) { \
		test_print(tree, TYPE_F); \
		fail("debug check nonzero", "true"); \
	} \
} while (false)

static void
simple_check_get_offset()
{
	header();

	struct matras_stats stats;
	matras_stats_create(&stats);
	stats.extent_count = extents_count;

	const int rounds = 2000;
	test tree;
	test_create(&tree, 0, extent_alloc, extent_free, &extents_count,
		    &stats);

	printf("Insert 1..X, remove 1..X\n");

	for (int i = 0; i < rounds; i++)
		INSERT_AND_CHECK_GET_POS(&tree, i, (size_t)i);
	fail_unless(test_size(&tree) == (size_t)rounds);
	fail_unless((int)stats.extent_count == extents_count);

	for (int i = 0; i < rounds; i++)
		DELETE_AND_CHECK_GET_POS(&tree, i, 0);
	fail_unless(test_size(&tree) == 0);
	fail_unless((int)stats.extent_count == extents_count);

	printf("Insert 1..X, remove X..1\n");

	for (int i = 0; i < rounds; i++)
		INSERT_AND_CHECK_GET_POS(&tree, i, (size_t)i);
	fail_unless(test_size(&tree) == (size_t)rounds);
	fail_unless((int)stats.extent_count == extents_count);

	for (int i = rounds - 1; i >= 0; i--)
		DELETE_AND_CHECK_GET_POS(&tree, i, (size_t)i);
	fail_unless(test_size(&tree) == 0);
	fail_unless((int)stats.extent_count == extents_count);

	printf("Insert X..1, remove 1..X\n");

	for (int i = rounds - 1; i >= 0; i--)
		INSERT_AND_CHECK_GET_POS(&tree, i, 0);
	fail_unless(test_size(&tree) == (size_t)rounds);
	fail_unless((int)stats.extent_count == extents_count);

	for (int i = 0; i < rounds; i++)
		DELETE_AND_CHECK_GET_POS(&tree, i, 0);
	fail_unless(test_size(&tree) == 0);
	fail_unless((int)stats.extent_count == extents_count);

	printf("Insert X..1, remove X..1\n");

	for (int i = rounds - 1; i >= 0; i--)
		INSERT_AND_CHECK_GET_POS(&tree, i, 0);
	fail_unless(test_size(&tree) == (size_t)rounds);
	fail_unless((int)stats.extent_count == extents_count);

	for (int i = rounds - 1; i >= 0; i--)
		DELETE_AND_CHECK_GET_POS(&tree, i, (size_t)i);
	fail_unless(test_size(&tree) == 0);
	fail_unless((int)stats.extent_count == extents_count);

	test_destroy(&tree);

	footer();
}

#define INSERT_AND_CHECK_BY_POS(tree, v, pos_to_insert) do { \
	size_t pos; \
	fail_unless(test_find(tree, v) == NULL); \
	fail_unless(test_insert_by_offset(tree, v, pos_to_insert) == 0); \
	type_t *found = test_find(tree, v); \
	fail_unless(found != NULL); \
	fail_unless(test_find_get_offset(tree, v, &pos) == found); \
	fail_unless(pos == pos_to_insert); \
	fail_unless(test_find_by_offset(tree, pos) == found); \
	if (test_debug_check(tree)) { \
		test_print(tree, TYPE_F); \
		fail("debug check nonzero", "true"); \
	} \
} while (false)

#define DELETE_AND_CHECK_BY_POS(tree, v, pos_to_delete) do { \
	fail_unless(test_find(tree, v) != NULL); \
	fail_unless(test_delete_by_offset(tree, pos_to_delete) == 0); \
	fail_unless(test_find(tree, v) == NULL); \
	if (test_debug_check(tree)) { \
		test_print(tree, TYPE_F); \
		fail("debug check nonzero", "true"); \
	} \
} while (false)

static void
simple_check_by_offset()
{
	header();

	struct matras_stats stats;
	matras_stats_create(&stats);
	stats.extent_count = extents_count;

	const int rounds = 2000;
	test tree;
	test_create(&tree, 0, extent_alloc, extent_free, &extents_count,
		    &stats);

	printf("Insert 1..X, remove 1..X\n");

	for (int i = 0; i < rounds; i++)
		INSERT_AND_CHECK_BY_POS(&tree, i, (size_t)i);
	fail_unless(test_size(&tree) == (size_t)rounds);
	fail_unless((int)stats.extent_count == extents_count);

	for (int i = 0; i < rounds; i++)
		DELETE_AND_CHECK_BY_POS(&tree, i, 0);
	fail_unless(test_size(&tree) == 0);
	fail_unless((int)stats.extent_count == extents_count);

	printf("Insert 1..X, remove X..1\n");

	for (int i = 0; i < rounds; i++)
		INSERT_AND_CHECK_BY_POS(&tree, i, (size_t)i);
	fail_unless(test_size(&tree) == (size_t)rounds);
	fail_unless((int)stats.extent_count == extents_count);

	for (int i = rounds - 1; i >= 0; i--)
		DELETE_AND_CHECK_BY_POS(&tree, i, (size_t)i);
	fail_unless(test_size(&tree) == 0);
	fail_unless((int)stats.extent_count == extents_count);

	printf("Insert X..1, remove 1..X\n");

	for (int i = rounds - 1; i >= 0; i--)
		INSERT_AND_CHECK_BY_POS(&tree, i, 0);
	fail_unless(test_size(&tree) == (size_t)rounds);
	fail_unless((int)stats.extent_count == extents_count);

	for (int i = 0; i < rounds; i++)
		DELETE_AND_CHECK_BY_POS(&tree, i, 0);
	fail_unless(test_size(&tree) == 0);
	fail_unless((int)stats.extent_count == extents_count);

	printf("Insert X..1, remove X..1\n");

	for (int i = rounds - 1; i >= 0; i--)
		INSERT_AND_CHECK_BY_POS(&tree, i, 0);
	fail_unless(test_size(&tree) == (size_t)rounds);
	fail_unless((int)stats.extent_count == extents_count);

	for (int i = rounds - 1; i >= 0; i--)
		DELETE_AND_CHECK_BY_POS(&tree, i, (size_t)i);
	fail_unless(test_size(&tree) == 0);
	fail_unless((int)stats.extent_count == extents_count);

	test_destroy(&tree);

	footer();
}

#endif

static bool
check_trees_are_identical(test *tree, sptree_test *spt_test)
{
	if (test_size(tree) != spt_test->size)
		return false;
	int n = test_size(tree);
	test_iterator iterator = test_first(tree);
	sptree_test_iterator *spitr = sptree_test_iterator_init(spt_test);
	for (int i = 0; i < n; i++) {
		type_t v1 = *test_iterator_get_elem(tree, &iterator);
		type_t v2 = *(type_t *)sptree_test_iterator_next(spitr);
		test_iterator_next(tree, &iterator);
		if (v1 != v2) {
			sptree_test_iterator_free(spitr);
			return false;
		}
	}
	sptree_test_iterator_free(spitr);
	return true;
}

static void
compare_with_sptree_check()
{
	header();

	sptree_test spt_test;
	sptree_test_init(&spt_test, sizeof(type_t), 0, 0, 0, &node_comp, 0, 0);

	test tree;
	test_create(&tree, 0, extent_alloc, extent_free, &extents_count, NULL);

	const int rounds = 16 * 1024;
	const int elem_limit = 	1024;

	for (int i = 0; i < rounds; i++) {
		type_t rnd = rand() % elem_limit;
		int find_res1 = sptree_test_find(&spt_test, &rnd) ? 1 : 0;
		int find_res2 = test_find(&tree, rnd) ? 1 : 0;
		if (find_res1 ^ find_res2)
			fail("trees identity", "false");

		if (find_res1 == 0) {
			sptree_test_replace(&spt_test, &rnd, NULL);
			test_insert(&tree, rnd, 0, 0);
		} else {
			sptree_test_delete(&spt_test, &rnd);
			test_delete(&tree, rnd);
		}

		if (test_debug_check(&tree))
			fail("debug check nonzero", "true");

		if (!check_trees_are_identical(&tree, &spt_test))
			fail("trees identity", "false");
	}
	sptree_test_destroy(&spt_test);

	test_destroy(&tree);

	footer();
}

static void
compare_with_sptree_check_branches()
{
	header();

	sptree_test spt_test;
	sptree_test_init(&spt_test, sizeof(type_t), 0, 0, 0, &node_comp, 0, 0);

	test tree;
	test_create(&tree, 0, extent_alloc, extent_free, &extents_count, NULL);

	const int elem_limit = COMPARE_WITH_SPTREE_CHECK_BRANCHES_ELEM_LIMIT;

	for (int i = 0; i < elem_limit; i++) {
		type_t v = (type_t)i;
		int find_res1 = sptree_test_find(&spt_test, &v) ? 1 : 0;
		int find_res2 = test_find(&tree, v) ? 1 : 0;
		if (find_res1 ^ find_res2)
			fail("trees identity", "false");
		if (find_res1) {
			fail("trees integrity", "false");
		}

		sptree_test_replace(&spt_test, &v, NULL);
		test_insert(&tree, v, 0, 0);

		if (test_debug_check(&tree))
			fail("debug check nonzero", "true");
		if (!check_trees_are_identical(&tree, &spt_test))
			fail("trees identity", "false");
	}
	for (int i = 0; i < elem_limit; i++) {
		type_t v = (type_t)i;
		int find_res1 = sptree_test_find(&spt_test, &v) ? 1 : 0;
		int find_res2 = test_find(&tree, v) ? 1 : 0;
		if (find_res1 ^ find_res2)
			fail("trees identity", "false");
		if (!find_res1)
			fail("trees integrity", "false");

		sptree_test_delete(&spt_test, &v);
		test_delete(&tree, v);

		if (test_debug_check(&tree))
			fail("debug check nonzero", "true");
		if (!check_trees_are_identical(&tree, &spt_test))
			fail("trees identity", "false");
	}

	for (int i = 0; i < elem_limit; i++) {
		type_t v = (type_t)(elem_limit - i - 1);
		int find_res1 = sptree_test_find(&spt_test, &v) ? 1 : 0;
		int find_res2 = test_find(&tree, v) ? 1 : 0;
		if (find_res1 ^ find_res2)
			fail("trees identity", "false");
		if (find_res1)
			fail("trees integrity", "false");

		sptree_test_replace(&spt_test, &v, NULL);
		test_insert(&tree, v, 0, 0);

		if (test_debug_check(&tree))
			fail("debug check nonzero", "true");
		if (!check_trees_are_identical(&tree, &spt_test))
			fail("trees identity", "false");
	}
	for (int i = 0; i < elem_limit; i++) {
		type_t v = (type_t)(elem_limit - i - 1);
		int find_res1 = sptree_test_find(&spt_test, &v) ? 1 : 0;
		int find_res2 = test_find(&tree, v) ? 1 : 0;
		if (find_res1 ^ find_res2)
			fail("trees identity", "false");
		if (!find_res1)
			fail("trees integrity", "false");

		sptree_test_delete(&spt_test, &v);
		test_delete(&tree, v);

		if (test_debug_check(&tree))
			fail("debug check nonzero", "true");
		if (!check_trees_are_identical(&tree, &spt_test))
			fail("trees identity", "false");
	}

	for (int i = 0; i < elem_limit; i++) {
		type_t v = (type_t)i;
		int find_res1 = sptree_test_find(&spt_test, &v) ? 1 : 0;
		int find_res2 = test_find(&tree, v) ? 1 : 0;
		if (find_res1 ^ find_res2)
			fail("trees identity", "false");
		if (find_res1)
			fail("trees integrity", "false");

		sptree_test_replace(&spt_test, &v, NULL);
		test_insert(&tree, v, 0, 0);

		if (test_debug_check(&tree))
			fail("debug check nonzero", "true");
		if (!check_trees_are_identical(&tree, &spt_test))
			fail("trees identity", "false");
	}
	for (int i = 0; i < elem_limit; i++) {
		type_t v;
		if (i & 1)
			v = (type_t)(elem_limit / 2 + i / 2);
		else
			v = (type_t)(elem_limit / 2 - i / 2 - 1);
		int find_res1 = sptree_test_find(&spt_test, &v) ? 1 : 0;
		int find_res2 = test_find(&tree, v) ? 1 : 0;
		if (find_res1 ^ find_res2)
			fail("trees identity", "false");
		if (!find_res1)
			fail("trees integrity", "false");

		sptree_test_delete(&spt_test, &v);
		test_delete(&tree, v);

		if (test_debug_check(&tree))
			fail("debug check nonzero", "true");
		if (!check_trees_are_identical(&tree, &spt_test))
			fail("trees identity", "false");
	}

	for (int i = 0; i < elem_limit; i++) {
		type_t v = (type_t)i;
		int find_res1 = sptree_test_find(&spt_test, &v) ? 1 : 0;
		int find_res2 = test_find(&tree, v) ? 1 : 0;
		if (find_res1 ^ find_res2)
			fail("trees identity", "false");
		if (find_res1)
			fail("trees integrity", "false");

		sptree_test_replace(&spt_test, &v, NULL);
		test_insert(&tree, v, 0, 0);

		if (test_debug_check(&tree))
			fail("debug check nonzero", "true");
		if (!check_trees_are_identical(&tree, &spt_test))
			fail("trees identity", "false");
	}
	for (int i = 0; i < elem_limit; i++) {
		type_t v;
		if (i & 1)
			v = (type_t)(i / 2);
		else
			v = (type_t)(elem_limit - i / 2 - 1);
		int find_res1 = sptree_test_find(&spt_test, &v) ? 1 : 0;
		int find_res2 = test_find(&tree, v) ? 1 : 0;
		if (find_res1 ^ find_res2)
			fail("trees identity", "false");
		if (!find_res1)
			fail("trees integrity", "false");

		sptree_test_delete(&spt_test, &v);
		test_delete(&tree, v);

		if (test_debug_check(&tree))
			fail("debug check nonzero", "true");
		if (!check_trees_are_identical(&tree, &spt_test))
			fail("trees identity", "false");
	}

	for (int i = 0; i < elem_limit; i++) {
		type_t v;
		if (i < elem_limit / 2)
			v = (type_t)(i * 2);
		else
			v = (type_t)((i - elem_limit / 2) * 2 + 1);
		int find_res1 = sptree_test_find(&spt_test, &v) ? 1 : 0;
		int find_res2 = test_find(&tree, v) ? 1 : 0;
		if (find_res1 ^ find_res2)
			fail("trees identity", "false");
		if (find_res1)
			fail("trees integrity", "false");

		sptree_test_replace(&spt_test, &v, NULL);
		test_insert(&tree, v, 0, 0);

		if (test_debug_check(&tree))
			fail("debug check nonzero", "true");
		if (!check_trees_are_identical(&tree, &spt_test))
			fail("trees identity", "false");
	}
	for (int i = 0; i < elem_limit; i++) {
		type_t v;
		if (i < elem_limit / 2)
			v = (type_t)(i * 2);
		else
			v = (type_t)((i - elem_limit / 2) * 2 + 1);
		int find_res1 = sptree_test_find(&spt_test, &v) ? 1 : 0;
		int find_res2 = test_find(&tree, v) ? 1 : 0;
		if (find_res1 ^ find_res2)
			fail("trees identity", "false");
		if (!find_res1)
			fail("trees integrity", "false");

		sptree_test_delete(&spt_test, &v);
		test_delete(&tree, v);

		if (test_debug_check(&tree))
			fail("debug check nonzero", "true");
		if (!check_trees_are_identical(&tree, &spt_test))
			fail("trees identity", "false");
	}

	if (tree.common.debug_insert_leaf_branches_mask !=
	    tree.common.debug_insert_leaf_branches_max_mask)
		fail("not all insert leaf branches was tested", "true");
	if (tree.common.debug_insert_inner_branches_mask !=
	    tree.common.debug_insert_inner_branches_max_mask)
		fail("not all insert inner branches was tested", "true");
	if (tree.common.debug_delete_leaf_branches_mask !=
	    tree.common.debug_delete_leaf_branches_max_mask)
		fail("not all delete leaf branches was tested", "true");
	if (tree.common.debug_delete_inner_branches_mask !=
	    tree.common.debug_delete_inner_branches_max_mask)
		fail("not all delete inner branches was tested", "true");

	sptree_test_destroy(&spt_test);

	test_destroy(&tree);

	footer();
}

static void
bps_tree_debug_self_check()
{
	header();

	int res = test_debug_check_internal_functions(false);
	if (res)
		printf("self test returned error %d\n", res);

	test_debug_check_internal_functions(true);

	footer();
}

static void
loading_test()
{
	header();

	test tree;

	const type_t test_count = 1000;
	type_t arr[test_count];
	for (type_t i = 0; i < test_count; i++)
		arr[i] = i;

	for (type_t i = 0; i <= test_count; i++) {
		test_create(&tree, 0, extent_alloc, extent_free,
			    &extents_count, NULL);

		if (test_build(&tree, arr, i))
			fail("building failed", "true");

		if (test_debug_check(&tree))
			fail("debug check nonzero", "true");

		struct test_iterator iterator;
		iterator = test_first(&tree);
		for (type_t j = 0; j < i; j++) {
			type_t *v = test_iterator_get_elem(&tree, &iterator);
			if (!v || *v != j)
				fail("wrong build result", "true");
			test_iterator_next(&tree, &iterator);
		}
		if (!test_iterator_is_invalid(&iterator))
			fail("wrong build result", "true");

		test_destroy(&tree);
	}

	footer();
}

static void
printing_test()
{
	header();

	test tree;
	test_create(&tree, 0, extent_alloc, extent_free, &extents_count, NULL);

	const type_t rounds = 22;

	for (type_t i = 0; i < rounds; i++) {
		type_t v = rounds + i;
		printf("Inserting " TYPE_F "\n", v);
		test_insert(&tree, v, 0, 0);
		test_print(&tree, TYPE_F);
		v = rounds - i - 1;
		printf("Inserting " TYPE_F "\n", v);
		test_insert(&tree, v, 0, 0);
		test_print(&tree, TYPE_F);
	}

	test_destroy(&tree);

	footer();
}

static void
white_box_test()
{
	header();

	test tree;
	test_create(&tree, 0, extent_alloc, extent_free, &extents_count, NULL);

	const int count_in_leaf = BPS_TREE_test_MAX_COUNT_IN_LEAF;
	const int count_in_inner = BPS_TREE_test_MAX_COUNT_IN_INNER;

	assert(count_in_leaf == TEST_COUNT_IN_LEAF);
	assert(count_in_inner == TEST_COUNT_IN_INNER);

	printf("full leaf:\n");
	for (type_t i = 0; i < count_in_leaf; i++) {
		test_insert(&tree, i, 0, 0);
	}
	test_print(&tree, TYPE_F);
	fail_unless(tree.common.leaf_count == 1);

	printf("split now:\n");
	test_insert(&tree, count_in_leaf, 0, 0);
	test_print(&tree, TYPE_F);
	fail_unless(tree.common.leaf_count == 2);

	printf("full 2 leafs:\n");
	for (type_t i = count_in_leaf + 1; i < count_in_leaf * 2; i++) {
		test_insert(&tree, i, 0, 0);
	}
	test_print(&tree, TYPE_F);
	fail_unless(tree.common.leaf_count == 2);

	printf("split now:\n");
	test_insert(&tree, count_in_leaf * 2, 0, 0);
	test_print(&tree, TYPE_F);
	fail_unless(tree.common.leaf_count == 3);

	printf("full 3 leafs:\n");
	for (type_t i = count_in_leaf * 2 + 1; i < count_in_leaf * 3; i++) {
		test_insert(&tree, i, 0, 0);
	}
	test_print(&tree, TYPE_F);
	fail_unless(tree.common.leaf_count == 3);

	printf("split now:\n");
	test_insert(&tree, count_in_leaf * 3, 0, 0);
	test_print(&tree, TYPE_F);
	fail_unless(tree.common.leaf_count == 4);

	test_destroy(&tree);

	test_create(&tree, 0, extent_alloc, extent_free, &extents_count, NULL);
	type_t arr[count_in_leaf * count_in_inner];
	for (type_t i = 0; i < count_in_leaf * count_in_inner; i++)
		arr[i] = i;
	test_build(&tree, arr, count_in_leaf * count_in_inner);
	printf("full %d leafs:\n", count_in_inner);
	test_print(&tree, TYPE_F);
	fail_unless(tree.common.leaf_count == count_in_inner);
	fail_unless(tree.common.inner_count == 1);

	printf("2-level split now:\n");
	test_insert(&tree, count_in_leaf * count_in_inner, 0, 0);
	test_print(&tree, TYPE_F);
	fail_unless(tree.common.inner_count == 3);

	test_destroy(&tree);

	footer();
}

static void
approximate_count()
{
	header();
	srand(0);

	approx tree;
	approx_create(&tree, 0, extent_alloc, extent_free, &extents_count,
		      NULL);

	uint32_t in_leaf_max_count = BPS_TREE_approx_MAX_COUNT_IN_LEAF;
	uint32_t in_leaf_min_count = in_leaf_max_count * 2 / 3;
	uint32_t in_leaf_ave_count = in_leaf_max_count * 5 / 6;
	uint32_t in_inner_max_count = BPS_TREE_approx_MAX_COUNT_IN_INNER;
	uint32_t in_inner_min_count = in_inner_max_count * 2 / 3;
	uint32_t in_inner_ave_count = in_inner_max_count * 5 / 6;
	double X = in_leaf_ave_count;
	double Y = in_inner_ave_count;
	double low_border_leaf = double(in_leaf_min_count) / in_leaf_ave_count;
	double upper_border_leaf = double(in_leaf_max_count) / in_leaf_ave_count;
	double low_border_inner = double(in_inner_min_count) / in_inner_ave_count;
	double upper_border_inner = double(in_inner_max_count) / in_inner_ave_count;

	const uint32_t short_sequence_count = 50;
	const uint32_t long_sequence_count = 30;
	const uint32_t long_sequence_multiplier = 20;
	const uint32_t arr_size = short_sequence_count *
				   (short_sequence_count + 1) / 2 +
				   long_sequence_count *
				   (long_sequence_count + 1) * long_sequence_multiplier / 2;
	uint64_t arr[arr_size];
	uint64_t count = 0;
	for (uint64_t i = 1; i <= short_sequence_count; i++)
		for (uint64_t j = 0; j < i; j++)
			arr[count++] = ((i * 100) << 32) | j;
	for (uint64_t i = 1; i <= long_sequence_count; i++)
		for (uint64_t j = 0; j < i * long_sequence_multiplier; j++)
			arr[count++] = ((i * 100 + 50) << 32) | j;
	printf("Count: %llu %u\n", (unsigned long long)count, arr_size);
	assert(count == arr_size);

	for (uint64_t i = 0; i < count * 10; i++) {
		uint64_t j = rand() % count;
		uint64_t k = rand() % count;
		uint64_t tmp = arr[j];
		arr[j] = arr[k];
		arr[k] = tmp;
	}

	for (uint64_t i = 0; i < count; i++)
		approx_insert(&tree, arr[i], NULL, NULL);

	printf("Count: %zu\n", approx_size(&tree));

	count = 0;
	int err_count = 0;
	const uint32_t over_possible =
		(short_sequence_count + long_sequence_count + 1) * 100;
	for (uint32_t i = 50; i < over_possible; i += 25) {
		uint64_t true_count = 0;
		if (i % 100 == 0) {
			uint64_t j = i / 100;
			if (j >= 1 && j <= short_sequence_count)
				true_count = j;
		} else if (i % 50 == 0) {
			uint64_t j = i / 100;
			if (j >= 1 && j <= long_sequence_count)
				true_count = j * long_sequence_multiplier;
		}
		count += true_count;

		uint64_t approx_count = approx_approximate_count(&tree, i);

		if (approx_count <= X) {
			if (approx_count != true_count) {
				err_count++;
				if (err_count <= 10)
					printf("searching %u found %llu expected %llu\n",
					       i, (unsigned long long)approx_count,
					       (unsigned long long)true_count);
			}
			continue;
		}
		double H = ceil(log(approx_count / X) / log(Y));
		double low = approx_count * low_border_leaf *
			pow(low_border_inner, H - 1);
		double up = approx_count * upper_border_leaf *
			pow(upper_border_inner, H - 1);
		if (true_count < low || true_count > up) {
			err_count++;
			if (err_count <= 10)
				printf("searching %u found %llu expected %llu\n",
				       i, (unsigned long long)approx_count,
				       (unsigned long long)true_count);
		}
	};

	printf("Error count: %d\n", err_count);
	fail_unless(err_count == 0);
	printf("Count: %llu\n", (unsigned long long)count);

	approx_destroy(&tree);

	footer();
}

static void
insert_get_iterator()
{
	header();

	test tree;
	test_create(&tree, 0, extent_alloc, extent_free, &extents_count, NULL);
	type_t value = 100000;

	bps_insert_and_check(test, &tree, value, NULL)
	type_t i = 0;
	for (; i < 10000; i += 2)
		bps_insert_and_check(test, &tree, i, NULL);
	for (i = -2; i > -10000; i -= 2)
		bps_insert_and_check(test, &tree, i, NULL);
	for (i = -9999; i < 10000; i += 2)
		bps_insert_and_check(test, &tree, i, NULL)

	footer();
}

static void
delete_value_check()
{
	header();
	struct_tree tree;
	struct_tree_create(&tree, 0, extent_alloc, extent_free, &extents_count,
			   NULL);
	struct elem_t e1 = {1, 1};
	struct_tree_insert(&tree, e1, NULL, NULL);
	struct elem_t e2 = {1, 2};
	if (struct_tree_delete_value(&tree, e2, NULL) == 0)
		fail("deletion of the non-identical element must fail", "false");
	if (struct_tree_find(&tree, 1) == NULL)
		fail("test non-identical element deletion failure", "false");
	if (struct_tree_delete_value(&tree, e1, NULL) != 0)
		fail("deletion of the identical element must not fail", "false");
	if (struct_tree_find(&tree, 1) != NULL)
		fail("test identical element deletion completion", "false");
	struct_tree_destroy(&tree);
	footer();
}

static void
insert_successor_test()
{
	header();
	test tree;

	size_t limits[] = {20, 2000};

	for (size_t i = 0; i < sizeof(limits) / sizeof(limits[0]); i++) {
		size_t limit = limits[i];
		test_create(&tree, 0, extent_alloc, extent_free, &extents_count,
			    NULL);

		for (size_t j = 0; j < limit; j++) {
			type_t v = 1 + rand() % (limit - 1);

			bool exact = false;
			test_iterator itr = test_lower_bound(&tree, v, &exact);

			type_t expect_replaced = 0;
			type_t expect_successor = 0;
			if (exact) {
				expect_replaced =
					*test_iterator_get_elem(&tree, &itr);
			} else if (!test_iterator_is_invalid(&itr)) {
				expect_successor =
					*test_iterator_get_elem(&tree, &itr);
			}

			type_t replaced = 0;
			type_t successor = 0;

			test_insert(&tree, v, &replaced, &successor);

			fail_unless(replaced == expect_replaced);
			fail_unless(successor == expect_successor);
		}

		test_destroy(&tree);
	}

	footer();
}

int
main(void)
{
	plan(1);
	header();

	simple_check();
#ifdef BPS_BLOCK_CHILD_POWER_ARRAY
	simple_check_get_offset();
	simple_check_by_offset();
#endif
	compare_with_sptree_check();
	compare_with_sptree_check_branches();
	bps_tree_debug_self_check();
	loading_test();
#ifndef BPS_BLOCK_CHILD_POWER_ARRAY
	printing_test();
#endif
	white_box_test();
	approximate_count();
	ok(extents_count == 0, "memory leak check");
	insert_get_iterator();
	delete_value_check();
	insert_successor_test();

	footer();
	return check_plan();
}
