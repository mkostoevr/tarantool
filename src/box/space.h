#ifndef TARANTOOL_BOX_SPACE_H_INCLUDED
#define TARANTOOL_BOX_SPACE_H_INCLUDED
/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
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
#include "user_def.h"
#include "space_def.h"
#include "small/rlist.h"
#include "bit/bit.h"
#include "engine.h"
#include "index.h"
#include "error.h"
#include "diag.h"
#include "iproto_constants.h"
#include "core/event.h"
#include "txn_event_trigger.h"
#include "arrow/abi.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

struct space;
struct engine;
struct sequence;
struct txn;
struct request;
struct port;
struct tuple;
struct tuple_format;
struct space_upgrade;
struct space_wal_ext;

struct space_vtab {
	/** Free a space instance. */
	void (*destroy)(struct space *);
	/** Return binary size of a space. */
	size_t (*bsize)(struct space *);

	int (*execute_replace)(struct space *, struct txn *,
			       struct request *, struct tuple **result);
	int (*execute_delete)(struct space *, struct txn *,
			      struct request *, struct tuple **result);
	int (*execute_update)(struct space *, struct txn *,
			      struct request *, struct tuple **result);
	int (*execute_upsert)(struct space *, struct txn *, struct request *);
	/**
	 * Executes a batch insert request.
	 *
	 * The implementation may move or release the input array and schema.
	 */
	int (*execute_insert_arrow)(struct space *space, struct txn *txn,
				    struct ArrowArray *array,
				    struct ArrowSchema *schema);

	int (*ephemeral_replace)(struct space *, const char *, const char *);

	int (*ephemeral_delete)(struct space *, const char *);

	int (*ephemeral_rowid_next)(struct space *, uint64_t *);

	void (*init_system_space)(struct space *);
	/**
	 * Initialize an ephemeral space instance.
	 */
	void (*init_ephemeral_space)(struct space *);
	/**
	 * Check an index definition for violation of
	 * various limits.
	 */
	int (*check_index_def)(struct space *, struct index_def *);
	/**
	 * Create an instance of space index. Used in alter
	 * space before commit to WAL. The created index is
	 * deleted with delete operator.
	 */
	struct index *(*create_index)(struct space *, struct index_def *);
	/**
	 * Called by alter when a primary key is added,
	 * after create_index is invoked for the new
	 * key and before the write to WAL.
	 */
	int (*add_primary_key)(struct space *);
	/**
	 * Called by alter when the primary key is dropped.
	 * Do whatever is necessary with the space object,
	 * to not crash in DML.
	 */
	void (*drop_primary_key)(struct space *);
	/**
	 * Check that all tuples stored in a space are compatible
	 * with the new format.
	 */
	int (*check_format)(struct space *space, struct tuple_format *format);
	/**
	 * Build a new index, primary or secondary, and fill it
	 * with tuples stored in the given space. The function is
	 * supposed to assure that all tuples conform to the new
	 * format.
	 *
	 * @param src_space   space to use as build source
	 * @param new_index   index to build
	 * @param new_format  format for validating tuples
	 * @param check_unique_constraint
	 *                    if this flag is set the build procedure
	 *                    must check the uniqueness constraint of
	 *                    the new index, otherwise the check may
	 *                    be optimized out even if the index is
	 *                    marked as unique
	 *
	 * @retval  0           success
	 * @retval -1           build failed
	 */
	int (*build_index)(struct space *src_space, struct index *new_index,
			   struct tuple_format *new_format,
			   bool check_unique_constraint);
	/**
	 * Exchange two index objects in two spaces. Used
	 * to update a space with a newly built index, while
	 * making sure the old index doesn't leak.
	 */
	void (*swap_index)(struct space *old_space, struct space *new_space,
			   uint32_t old_index_id, uint32_t new_index_id);
	/**
	 * Notify the engine about the changed space,
	 * before it's done, to prepare 'new_space' object.
	 */
	int (*prepare_alter)(struct space *old_space,
			     struct space *new_space);
	/**
	 * Notify the engine after altering a space and before replacing
	 * old_space with new_space in the space cache.
	 */
	void (*finish_alter)(struct space *old_space,
			     struct space *new_space);
	/** Prepares a space for online upgrade on alter. */
	int (*prepare_upgrade)(struct space *old_space,
			       struct space *new_space);
	/**
	 * Called right after removing a space from the cache.
	 * The engine should abort all transactions involving
	 * the space, because the space will be destroyed soon.
	 *
	 * This function isn't allowed to yield or fail.
	 */
	void (*invalidate)(struct space *space);
};

/**
 * Every event associated with a particular space is divided into two parts:
 * event, bound to the space by name, and event, bound by id.
 */
struct space_event {
	/** Event, bound by id. */
	struct event *by_id;
	/** Event, bound by name. */
	struct event *by_name;
};

struct space {
	/** Virtual function table. */
	const struct space_vtab *vtab;
	/** Cached runtime access information. */
	struct access access[BOX_USER_MAX];
	/** Engine used by this space. */
	struct engine *engine;
	/** Triggers fired before executing a request. */
	struct rlist before_replace;
	/** Triggers fired after space_replace() -- see txn_commit_stmt(). */
	struct rlist on_replace;
	/** User-defined before_replace triggers. */
	struct space_event before_replace_event;
	/** User-defined on_replace triggers. */
	struct space_event on_replace_event;
	/** User-defined transactional event triggers. */
	struct space_event txn_events[txn_event_id_MAX];
	/** SQL Trigger list. */
	struct sql_trigger *sql_triggers;
	/**
	 * The number of *enabled* indexes in the space.
	 *
	 * After all indexes are built, it is equal to the number
	 * of non-nil members of the index[] array.
	 */
	uint32_t index_count;
	/**
	 * There may be gaps index ids, i.e. index 0 and 2 may exist,
	 * while index 1 is not defined. This member stores the
	 * max id of a defined index in the space. It defines the
	 * size of index_map array.
	 */
	uint32_t index_id_max;
	/** Space meta. */
	struct space_def *def;
	/** Sequence attached to this space or NULL. */
	struct sequence *sequence;
	/** Auto increment field number. */
	uint32_t sequence_fieldno;
	/** Path to data in the auto-increment field. */
	char *sequence_path;
	/** Enable/disable triggers. */
	bool run_triggers;
	/**
	 * When the flag is set, the space executes recovery triggers
	 * (e.g. before_recovery_replace instead of before_replace).
	 */
	bool run_recovery_triggers;
	/** This space has foreign key constraints in its format. */
	bool has_foreign_keys;
	/**
	 * Space format, cannot be NULL.
	 */
	struct tuple_format *format;
	/**
	 * Sparse array of indexes defined on the space, indexed
	 * by id. Used to quickly find index by id (for SELECTs).
	 */
	struct index **index_map;
	/**
	 * Dense array of indexes defined on the space, in order
	 * of index id.
	 */
	struct index **index;
	/**
	 * If bit i is set, the unique constraint of index i must
	 * be checked before inserting a tuple into this space.
	 * Note, it isn't quite the same as index_opts::is_unique,
	 * as we don't need to check the unique constraint of
	 * a unique index in case the uniqueness of the indexed
	 * fields is guaranteed by another unique index.
	 */
	void *check_unique_constraint_map;
	/** List of space holders. This member is a property of space cache. */
	struct rlist space_cache_pin_list;
	/**
	 * List of all tx stories in the space.
	 */
	struct rlist memtx_stories;
	/**
	 * List of currently running long (yielding) space alter operations
	 * triggered by statements applied to this space (see alter_space_do),
	 * linked by space_alter_stmt::link.
	 *
	 * We must exclude such statements from snapshot because they haven't
	 * reached WAL yet and may actually fail. With MVCC off, such
	 * statements would be visible from a read view so we have to keep
	 * track of them separately.
	 */
	struct rlist alter_stmts;
	/** Space upgrade state or NULL. */
	struct space_upgrade *upgrade;
	/**
	 * Pointer to the WAL extension configuration for this space
	 * (i.e. if not NULL WAL entries may contain extra fields).
	 */
	struct space_wal_ext *wal_ext;
	/**
	 * List of collation identifier holders.
	 * Linked by `coll_id_cache_holder::in_space`.
	 */
	struct rlist coll_id_holders;
	/**
	 * A reference to the lua table representing this space. Only used
	 * on space drop rollback in order to keep the lua references to
	 * this object in sync. For more information see #9120.
	 */
	int lua_ref;
	/** The effective state of this space. */
	struct {
		/**
		 * The effective state of synchronous replication for this
		 * space.
		 */
		bool is_sync;
	} state;
};

/** Space alter statement. */
struct space_alter_stmt {
	/** Link in space::alter_stmts. */
	struct rlist link;
	/** Old tuple. */
	struct tuple *old_tuple;
	/** New tuple. */
	struct tuple *new_tuple;
};

/**
 * Remove all temporary triggers from all associated events.
 */
void
space_remove_temporary_triggers(struct space *space);

/**
 * Detach constraints from space. They can be reattached or deleted then.
 */
void
space_detach_constraints(struct space *space);

/**
 * Reattach space constraints.
 */
void
space_reattach_constraints(struct space *space);

/**
 * Pin in cache the collation identifiers that are referenced by space format
 * and/or indexes, so that they can't be deleted.
 */
void
space_pin_collations(struct space *space);

/**
 * Unpin collation identifiers.
 */
void
space_unpin_collations(struct space *space);

/**
 * Pin functional default field values in cache, so that they can't be deleted.
 */
void
space_pin_defaults(struct space *space);

/**
 * Unpin functional default field values.
 */
void
space_unpin_defaults(struct space *space);

/** Initialize a base space instance. */
int
space_create(struct space *space, struct engine *engine,
	     const struct space_vtab *vtab, struct space_def *def,
	     struct rlist *key_list, struct tuple_format *format);

/**
 * Finish space initialization after finishing initial recovery. If a space was
 * created during initial recovery some parts of its initialization was skipped
 * because they were not possible yet.
 * For example, all funcs are loaded after loading of all spaces, so if a space
 * depend on some func in initialization, we have skip that part and come back
 * later and call this function after initial recovery is finished.
 * Actually, for simplicity, that function should be called with every @a space
 * despite of its state, it will do no harm in any case. @a nothing argument
 * is not used.
 * Return 0 on success, -1 on error (diag is set) which actually means that
 * we cannot start - something is broken in snapshot.
 */
int
space_on_initial_recovery_complete(struct space *space, void *nothing);

/**
 * Finish space initialization after finishing final recovery.
 * See the comment to space_on_initial_recovery_complete() for
 * the function semantics and rationale.
 */
int
space_on_final_recovery_complete(struct space *space, void *nothing);

/**
 * Finish space initialization after finishing bootstrap.
 * See the comment to space_on_initial_recovery_complete() for
 * the function semantics and rationale.
 */
int
space_on_bootstrap_complete(struct space *space, void *nothing);

/** Get space ordinal number. */
static inline uint32_t
space_id(const struct space *space)
{
	return space->def->id;
}

/** Get space name. */
static inline const char *
space_name(const struct space *space)
{
	return space->def->name;
}

/** Return true if space is data-temporary. */
static inline bool
space_is_data_temporary(const struct space *space)
{
	return space_opts_is_data_temporary(&space->def->opts);
}

/** Return true if space is temporary. */
static inline bool
space_is_temporary(const struct space *space)
{
	return space_opts_is_temporary(&space->def->opts);
}

/** Return true if space is synchronous. */
static inline bool
space_is_sync(const struct space *space)
{
	return space->state.is_sync;
}

/** Return replication group id of a space. */
static inline uint32_t
space_group_id(const struct space *space)
{
	return space->def->opts.group_id;
}

/** Return true if space is local. */
static inline bool
space_is_local(const struct space *space)
{
	return space_group_id(space) == GROUP_LOCAL;
}

void
space_run_triggers(struct space *space, bool yesno);

/**
 * Get index by index id.
 * @return NULL if the index is not found.
 */
static inline struct index *
space_index(struct space *space, uint32_t id)
{
	if (id <= space->index_id_max)
		return space->index_map[id];
	return NULL;
}

/**
 * Get index by index name.
 *
 * @param space Space index belongs to.
 * @param index_name Name of index to be found.
 * @param index_name_len Length of index name.
 *
 * @retval NULL if the index is not found.
 */
static inline struct index *
space_index_by_name(struct space *space, const char *index_name,
		    uint32_t index_name_len)
{
	for(uint32_t i = 0; i < space->index_count; i++) {
		struct index *index = space->index[i];
		if (strlen(index->def->name) != index_name_len)
			continue;
		if (strncmp(index_name, index->def->name, index_name_len) == 0)
			return index;
	}
	return NULL;
}

/**
 * `space_index_by_name` for NULL-terminated index names.
 */
static inline struct index *
space_index_by_name0(struct space *space, const char *index_name)
{
	return space_index_by_name(space, index_name, strlen(index_name));
}

/**
 * Return true if the unique constraint must be checked for
 * the index with the given id before inserting a tuple into
 * the space.
 */
static inline bool
space_needs_check_unique_constraint(struct space *space, uint32_t index_id)
{
	return bit_test(space->check_unique_constraint_map, index_id);
}

/**
 * Return key_def of the index identified by id or NULL
 * if there is no such index.
 */
struct key_def *
space_index_key_def(struct space *space, uint32_t id);

/**
 * Look up the index by id.
 */
static inline struct index *
index_find(struct space *space, uint32_t index_id)
{
	struct index *index = space_index(space, index_id);
	if (index == NULL) {
		diag_set(ClientError, ER_NO_SUCH_INDEX_ID, index_id,
			 space_name(space));
		diag_log();
	}
	return index;
}

/**
 * Returns number of bytes used in memory by tuples in the space.
 */
size_t
space_bsize(struct space *space);

/** Get definition of the n-th index of the space. */
struct index_def *
space_index_def(struct space *space, int n);

/**
 * Check whether or not the current user can be granted
 * the requested access to the space.
 */
int
access_check_space(struct space *space, user_access_t access);

/**
 * Check if the `space_event' has registered triggers.
 */
static inline bool
space_event_has_triggers(struct space_event *space_event)
{
	return event_has_triggers(space_event->by_id) ||
	       event_has_triggers(space_event->by_name);
}

/**
 * Check if the space has registered on_replace triggers.
 */
static inline bool
space_has_on_replace_triggers(struct space *space)
{
	return !rlist_empty(&space->on_replace) ||
	       space_event_has_triggers(&space->on_replace_event);
}

/**
 * Check if the space has registered before_replace triggers.
 */
static inline bool
space_has_before_replace_triggers(struct space *space)
{
	return !rlist_empty(&space->before_replace) ||
	       space_event_has_triggers(&space->before_replace_event);
}

/**
 * Run on_replace triggers registered for a space.
 */
int
space_on_replace(struct space *space, struct txn *txn);

/**
 * Execute a DML request on the given space.
 */
int
space_execute_dml(struct space *space, struct txn *txn,
		  struct request *request, struct tuple **result);

static inline int
space_ephemeral_replace(struct space *space, const char *tuple,
			const char *tuple_end)
{
	return space->vtab->ephemeral_replace(space, tuple, tuple_end);
}

static inline int
space_ephemeral_delete(struct space *space, const char *key)
{
	return space->vtab->ephemeral_delete(space, key);
}

/**
 * Generic implementation of space_vtab::swap_index
 * that simply swaps the two indexes in index maps.
 */
void
generic_space_swap_index(struct space *old_space, struct space *new_space,
			 uint32_t old_index_id, uint32_t new_index_id);

static inline void
init_system_space(struct space *space)
{
	space->vtab->init_system_space(space);
}

static inline int
space_check_index_def(struct space *space, struct index_def *index_def)
{
	return space->vtab->check_index_def(space, index_def);
}

static inline struct index *
space_create_index(struct space *space, struct index_def *index_def)
{
	return space->vtab->create_index(space, index_def);
}

static inline int
space_add_primary_key(struct space *space)
{
	return space->vtab->add_primary_key(space);
}

static inline int
space_check_format(struct space *space, struct tuple_format *format)
{
	return space->vtab->check_format(space, format);
}

static inline void
space_drop_primary_key(struct space *space)
{
	space->vtab->drop_primary_key(space);
}

static inline int
space_build_index(struct space *src_space, struct space *new_space,
		  struct index *new_index)
{
	bool check = space_needs_check_unique_constraint(new_space,
							 new_index->def->iid);
	return src_space->vtab->build_index(src_space, new_index,
					    new_space->format, check);
}

static inline void
space_swap_index(struct space *old_space, struct space *new_space,
		 uint32_t old_index_id, uint32_t new_index_id)
{
	assert(old_space->vtab == new_space->vtab);
	return new_space->vtab->swap_index(old_space, new_space,
					   old_index_id, new_index_id);
}

static inline int
space_prepare_alter(struct space *old_space, struct space *new_space)
{
	assert(old_space->vtab == new_space->vtab);
	return new_space->vtab->prepare_alter(old_space, new_space);
}

static inline void
space_finish_alter(struct space *old_space, struct space *new_space)
{
	assert(old_space->vtab == new_space->vtab);
	new_space->vtab->finish_alter(old_space, new_space);
}

static inline int
space_prepare_upgrade(struct space *old_space, struct space *new_space)
{
	assert(old_space->vtab == new_space->vtab);
	return new_space->vtab->prepare_upgrade(old_space, new_space);
}

static inline void
space_invalidate(struct space *space)
{
	return space->vtab->invalidate(space);
}

static inline bool
space_is_memtx(const struct space *space)
{
	return space->engine->id == 0;
}

/** Return true if space is run under vinyl engine. */
static inline bool
space_is_vinyl(const struct space *space)
{
	return strcmp(space->engine->name, "vinyl") == 0;
}

/**
 * Check that the space is a system space, see also `space_id_is_system`.
 */
bool
space_is_system(const struct space *space);

struct field_def;
/**
 * Allocate and initialize a space.
 * @param space_def Space definition.
 * @param key_list List of index_defs.
 * @retval Space object.
 */
struct space *
space_new(struct space_def *space_def, struct rlist *key_list);

/**
 * Create an ephemeral space.
 * @param space_def Space definition.
 * @param key_list List of index_defs.
 * @retval Space object.
 *
 * Ephemeral spaces are invisible via public API and they
 * are not persistent. They are needed solely to do some
 * transient calculations.
 */
struct space *
space_new_ephemeral(struct space_def *space_def, struct rlist *key_list);

/** Destroy and free a space. */
void
space_delete(struct space *space);

/**
 * Call a visitor function on every space. Spaces are visited in order from
 * lowest space id to the highest, however, system spaces are visited first.
 * This is essential for correctly recovery from the snapshot.
 */
int
space_foreach(int (*func)(struct space *sp, void *udata), void *udata);

/** Update the state of synchronous replication for system spaces. */
void
system_spaces_update_is_sync_state(bool is_sync);

/**
 * Dump space definition (key definitions, key count)
 * for ALTER.
 */
void
space_dump_def(const struct space *space, struct rlist *key_list);

/** Rebuild index map in a space after a series of swap index. */
void
space_fill_index_map(struct space *space);

/** Add info about space to the error. */
static inline void
error_set_space(struct error *error, struct space_def *def)
{
	error_set_str(error, "space", def->name);
	error_set_uint(error, "space_id", def->id);
}

/** Destroy constraints that are defined in @a space format. */
void
space_cleanup_constraints(struct space *space);

/*
 * Virtual method stubs.
 */
size_t generic_space_bsize(struct space *);
int generic_space_execute_insert_arrow(struct space *space, struct txn *txn,
				       struct ArrowArray *array,
				       struct ArrowSchema *schema);
int generic_space_ephemeral_replace(struct space *, const char *, const char *);
int generic_space_ephemeral_delete(struct space *, const char *);
int generic_space_ephemeral_rowid_next(struct space *, uint64_t *);
void generic_init_system_space(struct space *);
void generic_init_ephemeral_space(struct space *);
int generic_space_check_index_def(struct space *, struct index_def *);
int generic_space_add_primary_key(struct space *space);
void generic_space_drop_primary_key(struct space *space);
int generic_space_check_format(struct space *, struct tuple_format *);
int generic_space_build_index(struct space *, struct index *,
			      struct tuple_format *, bool);
int generic_space_prepare_alter(struct space *, struct space *);
void generic_space_finish_alter(struct space *, struct space *);
int generic_space_prepare_upgrade(struct space *old_space,
				  struct space *new_space);
void generic_space_invalidate(struct space *);

#if defined(__cplusplus)
} /* extern "C" */

static inline struct space *
space_new_xc(struct space_def *space_def, struct rlist *key_list)
{
	struct space *space = space_new(space_def, key_list);
	if (space == NULL)
		diag_raise();
	return space;
}

static inline void
access_check_space_xc(struct space *space, user_access_t access)
{
	if (access_check_space(space, access) != 0)
		diag_raise();
}

static inline void
space_check_index_def_xc(struct space *space, struct index_def *index_def)
{
	if (space_check_index_def(space, index_def) != 0)
		diag_raise();
}

static inline struct index *
space_create_index_xc(struct space *space, struct index_def *index_def)
{
	struct index *index = space_create_index(space, index_def);
	if (index == NULL)
		diag_raise();
	return index;
}

static inline void
space_add_primary_key_xc(struct space *space)
{
	if (space_add_primary_key(space) != 0)
		diag_raise();
}

static inline void
space_check_format_xc(struct space *space, struct tuple_format *format)
{
	if (space_check_format(space, format) != 0)
		diag_raise();
}

static inline void
space_build_index_xc(struct space *src_space, struct space *new_space,
		     struct index *new_index)
{
	if (space_build_index(src_space, new_space, new_index) != 0)
		diag_raise();
}

static inline void
space_prepare_alter_xc(struct space *old_space, struct space *new_space)
{
	if (space_prepare_alter(old_space, new_space) != 0)
		diag_raise();
}

static inline void
space_prepare_upgrade_xc(struct space *old_space, struct space *new_space)
{
	if (space_prepare_upgrade(old_space, new_space) != 0)
		diag_raise();
}

#endif /* defined(__cplusplus) */

#endif /* TARANTOOL_BOX_SPACE_H_INCLUDED */
