/*
 * Copyright 2010-2023, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "cfg.h"

#include "exception.h"
#include <cfg.h>
#include "main.h"
#include "lua/utils.h"
#include "box/lua/tuple.h"
#include "box/memtx_allocator.h"
#include "small/region.h"
#include "fiber.h"
#include "misc.h"

#include "box/box.h"
#include "libeio/eio.h"

#include <benchmark/benchmark.h>

#include <sched.h>
#include <unistd.h>

extern "C" {
	#include <lua.h>
} // extern "C"

uint32_t replace_space_id;
const char *replace_tuple;
size_t replace_tuple_len;

#if 0
static void DumpHex(const void* data, size_t size) {
	char ascii[17];
	size_t i, j;
	ascii[16] = '\0';
	for (i = 0; i < size; ++i) {
		printf("%02X ", ((unsigned char*)data)[i]);
		if (((unsigned char*)data)[i] >= ' ' && ((unsigned char*)data)[i] <= '~') {
			ascii[i % 16] = ((unsigned char*)data)[i];
		} else {
			ascii[i % 16] = '.';
		}
		if ((i+1) % 8 == 0 || i+1 == size) {
			printf(" ");
			if ((i+1) % 16 == 0) {
				printf("|  %s \n", ascii);
			} else if (i+1 == size) {
				ascii[(i+1) % 16] = '\0';
				if ((i+1) % 16 <= 8) {
					printf(" ");
				}
				for (j = (i+1) % 16; j < 16; ++j) {
					printf("   ");
				}
				printf("|  %s \n", ascii);
			}
		}
	}
}
#endif

void
invalidate_data_cache()
{
	const int size = 20 * 1024 * 1024; /* 20MB */
	char *c = (char *)malloc(size);
	for (int i = 0; i < 0xffff; i++)
		for (int j = 0; j < size; j++)
			c[j] = i*j;
}

static int
set_thread_affinity(int core_id) {
   int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
   if (core_id < 0 || core_id >= num_cores)
      return EINVAL;

   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(core_id, &cpuset);

   pthread_t current_thread = pthread_self();
   return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

extern "C" void invalidate_instruction_cache(void);

static void
benchmark_replace(benchmark::State& state)
{
	uint32_t space_id;

again:
	luaL_dostring(tarantool_L, "tester = box.schema.space.create(\"whatever\", {engine = \"memtx\"});"
				   "tester:create_index(\"pk\", {unique = true, type = \"TREE\", parts = {{1, \"double\"}}});"
				   "return tester.id");
	space_id = lua_tointeger(tarantool_L, -1);
	//printf("\n\n%d\n\n", space_id);
	lua_pop(tarantool_L, 1);
	if (space_id == 0) {
		sleep(1);
		luaL_dostring(tarantool_L, "tester:drop()");
		goto again;
	}

	//invalidate_data_cache();
	//invalidate_instruction_cache();

	size_t total_count = 0;
	for (auto _ : state) {
		struct tuple *dummy;
		box_replace(space_id, replace_tuple, replace_tuple + replace_tuple_len, &dummy);
		double old_value = *(double *)(replace_tuple + 2);
		*(double *)(replace_tuple + 2) = old_value + 0.1;
		total_count++;
	}

	//luaL_dostring(tarantool_L, "tester:drop(); require('fiber').yield()");
	luaL_dostring(tarantool_L, "tester:drop()");

	state.SetItemsProcessed(total_count);
}

BENCHMARK(benchmark_replace);

static int
lbox_benchmark_replace(struct lua_State *L)
{
	if (lua_gettop(L) != 2 || !lua_isnumber(L, 1) ||
	    (lua_type(L, 2) != LUA_TTABLE && luaT_istuple(L, 2) == NULL))
		return luaL_error(L, "Usage box.benchmark.benchmark_replace"
				     "(space_id, tuple)");

	set_thread_affinity(5);

	uint32_t space_id = lua_tonumber(L, 1);

	size_t tuple_len;
	size_t region_svp = region_used(&fiber()->gc);
	const char *tuple = lbox_encode_tuple_on_gc(L, 2, &tuple_len);
	if (tuple == NULL)
		return luaT_error(L);

	replace_space_id = space_id;
	replace_tuple = tuple;
	replace_tuple_len = tuple_len;

	char *argv[] = {
		strdup("benchmark_replace"),
		strdup("--benchmark_filter=benchmark_replace"),
		strdup("--benchmark_repetitions=63"),
		strdup("--benchmark_report_aggregates_only=true"),
		strdup("--benchmark_display_aggregates_only=true"),
		NULL
	};
	int argc = sizeof(argv) / sizeof(*argv);
	benchmark::Initialize(&argc, argv);
	benchmark::RunSpecifiedBenchmarks();

	for (int i = 0; i < argc; i++)
		free(argv[argc]);
	region_truncate(&fiber()->gc, region_svp);

	return 0;
}

extern "C" void
box_lua_benchmark_init(struct lua_State *L)
{
	static const struct luaL_Reg benchmarklib_internal[] = {
		{"replace", lbox_benchmark_replace},
		{NULL, NULL}
	};

	luaL_findtable(L, LUA_GLOBALSINDEX, "box.benchmark", 0);
	luaL_setfuncs(L, benchmarklib_internal, 0);
	lua_pop(L, 1);
}
