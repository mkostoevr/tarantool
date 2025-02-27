#include "memtx_sort_data.h"
#include "memtx_index_read_view.h"

#include "core/assoc.h"
#include "trivia/util.h"

/**
 * Map: (space_id, index_id) => (sort data entry information).
 */
struct memtx_sort_data_key {
	/** The space ID. */
	uint32_t space_id;
	/** The index ID. */
	uint32_t index_id;
};

static inline uint64_t
memtx_sort_data_key_hash(struct memtx_sort_data_key key)
{
	return (key.space_id << 8) | key.index_id;
}

static inline int
memtx_sort_data_key_cmp(struct memtx_sort_data_key a,
			struct memtx_sort_data_key b)
{
	return a.space_id != b.space_id || a.index_id != b.index_id;
}

#define mh_name _memtx_sort_data_entries
#define mh_key_t struct memtx_sort_data_key

/** The sort data file header entry (the index sort data information). */
struct memtx_sort_data_entry {
	/** The entry identifier. */
	struct memtx_sort_data_key key;
	/** The offset of the sort data in the file. */
	size_t offset;
	/** The phisical size of the sort data. */
	size_t psize;
	/** The amount of tuples stored. */
	size_t len;
	/** Writer-only: offset of offset field in the file header. */
	size_t offset_offset;
	/** Writer-only: offset of psize field in the file header. */
	size_t psize_offset;
	/** Writer-only: offset of len field in the file header. */
	size_t len_offset;
	/** Writer-only: the data is fully written into the file already. */
	bool is_committed;
};

#define mh_node_t struct memtx_sort_data_entry
#define mh_arg_t void *
#define mh_hash(a, arg) (memtx_sort_data_key_hash((a)->key))
#define mh_hash_key(a, arg) (memtx_sort_data_key_hash(a))
#define mh_cmp(a, b, arg) (memtx_sort_data_key_cmp((a)->key, (b)->key))
#define mh_cmp_key(a, b, arg) (memtx_sort_data_key_cmp(a, (b)->key))
#define MH_SOURCE
#include "salad/mhash.h"

/** The sort data file reader and writer context. */
struct memtx_sort_data {
	/** The sort data file pointer. */
	FILE *fp;
	/** The sort data file name. */
	char *fname;
	/** The memtx snapshot dir name. */
	char *dirname;
	/** The vclock signature of the sort data file. */
	int64_t signature;
	/** The sort data file instance signature. */
	const struct tt_uuid *instance_uuid;
	/** The information about the sort data entries. */
	struct mh_memtx_sort_data_entries_t *entries;
	/** The currently handled sort data entry. */
	struct memtx_sort_data_entry *curr_entry;
	/** The total amount of tuples in saved spaces. */
	size_t cardinality;
	/** The offset to the "Cardinality" file header key. */
	size_t cardinality_offset;
};

struct memtx_sort_data_reader {
	/** The sort data information. */
	struct memtx_sort_data *msd;
	/** The number of elements remained in the file. */
	size_t curr_entry_len_remained;
	/** The buffer to pre-read PK data into. */
	void **buffer;
	/** The buffer to pre-read PK data into (capacity). */
	size_t buffer_capacity;
	/** The buffer to pre-read PK data into (readable size). */
	size_t buffer_size;
	/** The buffer to pre-read PK data into (current pointer). */
	size_t buffer_i;
	/** The old to new tuple address map used on recovery. */
	struct mh_ptrptr_t *old2new;
};

struct memtx_sort_data *
memtx_sort_data_new_empty(const char *dirname,
			  const struct vclock *vclock,
			  const struct tt_uuid *instance_uuid)
{
	struct memtx_sort_data *msd = xcalloc(1, sizeof(*msd));
	msd->dirname = xstrdup(dirname);
	msd->signature = vclock_sum(vclock);
	msd->instance_uuid = instance_uuid;
	msd->entries = mh_memtx_sort_data_entries_new();
	msd->fname = xstrdup(
		tt_snprintf(PATH_MAX, "%s/%020lld.sortdata",
			    msd->dirname, (long long)msd->signature));
	return msd;
}

static void
memtx_sort_data_invalidate(struct memtx_sort_data *msd)
{
	msd->curr_entry = NULL; /* This is checked everywhere it's needed. */
	mh_memtx_sort_data_entries_clear(msd->entries);
}

/* {{{ Writer *****************************************************************/

struct memtx_sort_data *
memtx_sort_data_new(struct read_view *rv, const char *dirname,
		    struct tt_uuid *instance_uuid)
{
	struct memtx_sort_data *msd =
		memtx_sort_data_new_empty(dirname, &rv->vclock, instance_uuid);

	/* Find out which spaces and indexes must be saved. */
	struct space_read_view *space_rv;
	read_view_foreach_space(space_rv, rv) {
		/* Don't save the sort data of system spaces. */
		if (space_id_is_system(space_rv->id))
			continue;

		/* Only create sort data for spaces with SKs. */
		bool need_sort_data = false;
		for (uint32_t i = 1; i <= space_rv->index_id_max; i++) {
			/* No read view created - skip. */
			if (space_rv->index_map[i] == NULL)
				continue;

			/* Not a memtx index - skip the whole space. */
			if (strcmp(space_rv->index_map[i]->def->engine_name,
				   "memtx") != 0) {
				break;
			}

			/* Does not support the sort data - skip. */
			struct memtx_index_read_view *rv =
				(struct memtx_index_read_view *)
					space_rv->index_map[i];
			if (rv->dump_sort_data == NULL)
				continue;

			/* Ok, let's do this. */
			struct memtx_sort_data_entry entry = {};
			entry.key.space_id = space_rv->id;
			entry.key.index_id = i;
			mh_memtx_sort_data_entries_put(msd->entries,
						       &entry, NULL, NULL);
			need_sort_data = true;
		}

		/* Insert the PK entry if required. */
		if (need_sort_data) {
			struct memtx_sort_data_entry entry = {};
			entry.key.space_id = space_rv->id;
			entry.key.index_id = 0; /* Just for clarity. */
			mh_memtx_sort_data_entries_put(msd->entries,
						       &entry, NULL, NULL);
		}
	}

	return msd;
}

void
memtx_sort_data_delete(struct memtx_sort_data *msd)
{
	assert(msd->fp == NULL);
	mh_memtx_sort_data_entries_delete(msd->entries);
	free(msd->dirname);
	free(msd->fname);
	free(msd);
}

int
memtx_sort_data_open(struct memtx_sort_data *msd)
{
	msd->fp = fopen(msd->fname, "wb");
	if (msd->fp == NULL)
		return -1;

	/* Write the generic information. */
	fprintf(msd->fp, "SORTDATA\n");
	fprintf(msd->fp, "1\n");
	fprintf(msd->fp, "Version: %s\n", PACKAGE_VERSION);
	fprintf(msd->fp, "Instance: %s\n", tt_uuid_str(msd->instance_uuid));

	/* The overall tuple count is to be updated later. */
	fprintf(msd->fp, "Cardinality: ");
	msd->cardinality_offset = ftell(msd->fp);
	fprintf(msd->fp, "%020lld\n", 0LL);

	/* Write all the sort data entries. */
	fprintf(msd->fp, "Entries: %u\n", mh_size(msd->entries));
	mh_int_t i;
	mh_foreach(msd->entries, i) {
		struct memtx_sort_data_entry *entry =
			mh_memtx_sort_data_entries_node(msd->entries, i);
		fprintf(msd->fp, "%u/%u: ", entry->key.space_id,
			entry->key.index_id);
		entry->offset_offset = ftell(msd->fp);
		fprintf(msd->fp, "%016llx, ", 0LLU);
		entry->psize_offset = ftell(msd->fp);
		fprintf(msd->fp, "%016llx, ", 0LLU);
		entry->len_offset = ftell(msd->fp);
		fprintf(msd->fp, "%020llu\n", 0LLU);
	}
	fprintf(msd->fp, "\n");
	return 0;
}

void
memtx_sort_data_close(struct memtx_sort_data *msd)
{
	/* TODO: handle error. */
	fseek(msd->fp, msd->cardinality_offset, SEEK_SET);
	fprintf(msd->fp, "%020lld\n", (long long)msd->cardinality);
	fclose(msd->fp);
	msd->fp = NULL;
}

void
memtx_sort_data_discard(struct memtx_sort_data *msd)
{
	memtx_sort_data_close(msd);
	unlink(msd->fname);
}

bool
memtx_sort_data_begin(struct memtx_sort_data *msd,
		      uint32_t space_id, uint32_t index_id)
{
	assert(msd->curr_entry == NULL);
	struct memtx_sort_data_key key = {space_id, index_id};
	mh_int_t i = mh_memtx_sort_data_entries_find(msd->entries, key, NULL);
	if (i == mh_end(msd->entries))
		return false; /* Not included index. */

	struct memtx_sort_data_entry *entry =
		mh_memtx_sort_data_entries_node(msd->entries, i);
	assert(!entry->is_committed);
	if (fseek(msd->fp, 0, SEEK_END) != 0) {
		say_error("%s: space %u: index #%u seek failed, file"
			  " ignored", msd->fname, space_id, index_id);
		memtx_sort_data_invalidate(msd);
		return false;
	}
	entry->offset = ftell(msd->fp);
	entry->psize = 0; /* Just for clarity. */
	entry->len = 0; /* Ditto. */
	msd->curr_entry = entry;
	return true;
}

int
memtx_sort_data_write(struct memtx_sort_data *msd,
		      void *data, size_t size, size_t count)
{
	if (msd->curr_entry == NULL)
		return 0; /* Not included index. */

	if (fwrite(data, size, count, msd->fp) != count)
		return -1;
	msd->curr_entry->psize += size * count;
	msd->curr_entry->len += count;
	return 0;
}

void
memtx_sort_data_commit(struct memtx_sort_data *msd)
{
	if (msd->curr_entry == NULL)
		return; /* Not included index. */

	fseek(msd->fp, msd->curr_entry->offset_offset, SEEK_SET);
	fprintf(msd->fp, "%016llx, ", (long long)msd->curr_entry->offset);
	fseek(msd->fp, msd->curr_entry->psize_offset, SEEK_SET);
	fprintf(msd->fp, "%016llx, ", (long long)msd->curr_entry->psize);
	fseek(msd->fp, msd->curr_entry->len_offset, SEEK_SET);
	fprintf(msd->fp, "%020llu\n", (long long)msd->curr_entry->len);
	/* Only count PK tuples in cardinality. */
	if (msd->curr_entry->key.index_id == 0)
		msd->cardinality += msd->curr_entry->len;
	msd->curr_entry->is_committed = true;
	msd->curr_entry = NULL;
}

/* }}} */

/* {{{ Reader *****************************************************************/

/**
 * TODO
 */
static void
memtx_sort_data_reader_invalidate(struct memtx_sort_data_reader *msdr)
{
	memtx_sort_data_invalidate(msdr->msd);

	/* Re-create the old2new map so it does not comsume memory. */
	mh_ptrptr_delete(msdr->old2new);
	msdr->old2new = mh_ptrptr_new();
}

static bool
memtx_sort_data_parse_entry(const char *fname, const char *line,
			    const char *entry_name, int base,
			    const char *entry_ptr, char **after_ptr,
			    const char *expect_after, long *result)
{
	*result = strtol(entry_ptr, after_ptr, base);
	if (memcmp(*after_ptr, expect_after, strlen(expect_after))) {
		say_error("%s: expected '%s' after %s:\n\t%s",
			  fname, expect_after, entry_name, line);
		return false;
	}
	*after_ptr += strlen(expect_after);
	return true;
}

struct memtx_sort_data_reader *
memtx_sort_data_start_read(const char *dirname, const struct vclock *vclock,
			   const struct tt_uuid *instance_uuid)
{
	struct memtx_sort_data_reader *msdr = xcalloc(1, sizeof(*msdr));
	msdr->msd = memtx_sort_data_new_empty(dirname, vclock, instance_uuid);
	msdr->curr_entry_len_remained = 0;
	msdr->buffer_capacity = 1024 * 1024; /* In elements. */
	msdr->buffer = xcalloc(msdr->buffer_capacity, sizeof(*msdr->buffer));
	msdr->buffer_size = 0;
	msdr->buffer_i = 0;
	msdr->old2new = mh_ptrptr_new();

	/* Open the sort data file for read. */
	struct memtx_sort_data *msd = msdr->msd;
	msd->fp = fopen(msd->fname, "rb");
	if (msd->fp == NULL)
		goto fail_free; /* No sort data file found. */

	/* Read the file header and fill the entry info. */
	char tmp[256] = {};
	const char *hdr_magic = "SORTDATA\n";
	const char *hdr_version = "1\n";

	/* Check the file magic. */
	if (fgets(tmp, sizeof(tmp), msd->fp) == NULL) {
		say_error("%s: failed to read file magic", msd->fname);
		goto fail_close;
	}
	if (strcmp(tmp, hdr_magic) != 0) {
		say_error("%s: file magic is invalid", msd->fname);
		goto fail_close;
	}

	/* Check the file version. */
	if (fgets(tmp, sizeof(tmp), msd->fp) == NULL) {
		say_error("%s: failed to read file version", msd->fname);
		goto fail_close;
	}
	if (strcmp(tmp, hdr_version) != 0) {
		say_error("%s: file version is unsupported", msd->fname);
		goto fail_close;
	}

	/* Get the header keys. */
	const char *key_version = "Version: ";
	const char *key_instance = "Instance: ";
	const char *key_cardinality = "Cardinality: ";
	const char *key_entries = "Entries: ";
	uint32_t read_entries = 0;
	while (fgets(tmp, sizeof(tmp), msd->fp)) {
		if (memcmp(tmp, key_version, strlen(key_version)) == 0) {
			/* The version key is not handled yet. */
			continue;
		} else if (memcmp(tmp, key_instance,
				  strlen(key_instance)) == 0) {
			char *uuid_str = tmp + strlen(key_instance);
			const size_t uuid_len = strlen(uuid_str) - 1; /* \n */
			if (uuid_len != UUID_STR_LEN) {
				say_error("%s: invalid UUID size", msd->fname);
				goto fail_close;
			}
			uuid_str[uuid_len] = '\0';
			struct tt_uuid uuid;
			if (tt_uuid_from_string(uuid_str, &uuid) != 0) {
				say_error("%s: invalid UUID", msd->fname);
				goto fail_close;
			}
			/* TODO: do we need the nil check? */
			if (!tt_uuid_is_nil(msd->instance_uuid) &&
			    !tt_uuid_is_equal(&uuid, msd->instance_uuid)) {
				say_error("%s: not matched UUID", msd->fname);
				goto fail_close;
			}
		} else if (memcmp(tmp, key_cardinality,
				  strlen(key_cardinality)) == 0) {
			const char *cardinality_str = tmp +
						      strlen(key_cardinality);
			char *eol;
			msd->cardinality = strtol(cardinality_str, &eol, 10);
			if (*eol != '\n') {
				say_error("%s: invalid value for sort data "
					  "cardinality: %s", msd->fname, tmp);
				goto fail_close;
			}
			mh_ptrptr_reserve(msdr->old2new,
					  msd->cardinality, NULL);
		} else if (memcmp(tmp, key_entries, strlen(key_entries)) == 0) {
			const char *entry_count_str = tmp + strlen(key_entries);
			read_entries = atoi(entry_count_str);
		} else if (read_entries) {
			char *space_id_str = tmp;
			char *index_id_str;
			long space_id;
			if (!memtx_sort_data_parse_entry(msd->fname, tmp,
							 "space ID", 10,
							 space_id_str,
							 &index_id_str,
							 "/", &space_id)) {
				goto fail_close;
			}

			char *offset_str;
			long index_id;
			if (!memtx_sort_data_parse_entry(msd->fname, tmp,
							 "index ID", 10,
							 index_id_str,
							 &offset_str,
							 ": ", &index_id)) {
				goto fail_close;
			}

			char *psize_str;
			long offset;
			if (!memtx_sort_data_parse_entry(msd->fname, tmp,
							 "data offset", 16,
							 offset_str, &psize_str,
							 ", ", &offset)) {
				goto fail_close;
			}

			char *len_str;
			long psize;
			if (!memtx_sort_data_parse_entry(msd->fname, tmp,
							 "phisical size", 16,
							 psize_str, &len_str,
							 ", ", &psize)) {
				goto fail_close;
			}

			char *end;
			uint64_t len = strtol(len_str, &end, 10);
			if (*end != '\n') {
				say_warn("%s: unexpected contents in the "
					 "sort data key, index skipped: %s",
					 msd->fname, tmp);
				continue;
			}

			/* Sanity check. */
			if ((len == 0) != (psize == 0)) {
				say_error("%s: entry size verification"
					  " failed", msd->fname);
				goto fail_close;
			}

			struct memtx_sort_data_entry entry = {};
			entry.key.space_id = space_id;
			entry.key.index_id = index_id;
			entry.offset = offset;
			entry.psize = psize;
			entry.len = len;
			mh_memtx_sort_data_entries_put(msd->entries,
						       &entry, NULL, NULL);

			read_entries--;
		} else if (tmp[0] == '\n') {
			break;
		}
	}
	say_info("using the memtx sort data from `%s'", msd->fname);
	return msdr;

fail_close:
	fclose(msd->fp);
	msd->fp = NULL;
fail_free:
	say_warn("memtx sort data file `%s' ignored", msd->fname);
	memtx_sort_data_delete(msd);
	mh_ptrptr_delete(msdr->old2new);
	free(msdr->buffer);
	free(msdr);
	return NULL;
}

void
memtx_sort_data_init_pk(struct memtx_sort_data_reader *msdr, uint32_t space_id)
{
	struct memtx_sort_data *msd = msdr->msd;
	struct memtx_sort_data_key key = {space_id, 0};
	mh_int_t i = mh_memtx_sort_data_entries_find(msd->entries, key, NULL);
	if (i == mh_end(msd->entries)) {
		msd->curr_entry = NULL; /* No sort data or it's corrupted. */
		return;
	}

	/* Seek to the PK data and save the entry info to access later. */
	struct memtx_sort_data_entry *entry =
		mh_memtx_sort_data_entries_node(msd->entries, i);
	if (fseek(msd->fp, entry->offset, SEEK_SET) != 0) {
		/* The sort data is corrupted, clear all the info. */
		say_error("%s: space %u: PK seek failed, file ignored",
			  msd->fname, msd->curr_entry->key.space_id);
		memtx_sort_data_reader_invalidate(msdr);
		return;
	}
	msd->curr_entry = entry;

	/* That's how much we're planning to read. */
	assert(msdr->curr_entry_len_remained == 0);
	msdr->curr_entry_len_remained = entry->len;
}

void
memtx_sort_data_map_next_pk_tuple(struct memtx_sort_data_reader *msdr,
				  struct tuple *new_ptr)
{
	struct memtx_sort_data *msd = msdr->msd;
	if (msd->curr_entry == NULL)
		return; /* No sort data or it's corrupted. */

	struct mh_ptrptr_node_t node;
	if (msdr->buffer_i >= msdr->buffer_size) {
		/* We must have PK data to read if we've requested one. */
		assert((size_t)ftell(msd->fp) <
		       msd->curr_entry->offset + msd->curr_entry->psize);
		msdr->buffer_size = MIN(msdr->buffer_capacity,
					msdr->curr_entry_len_remained);
		if (fread(msdr->buffer, sizeof(*msdr->buffer),
			  msdr->buffer_size, msd->fp) != msdr->buffer_size) {
			/* The sort data is corrupted, clear all the info. */
			say_error("%s: space %u: PK read failed, file ignored",
				  msd->fname, msd->curr_entry->key.space_id);
			memtx_sort_data_reader_invalidate(msdr);
			return;
		}
		msdr->curr_entry_len_remained -= msdr->buffer_size;
		msdr->buffer_i = 0;
	}
	node.key = msdr->buffer[msdr->buffer_i++];
	node.val = new_ptr;
	mh_ptrptr_put(msdr->old2new, &node, NULL, NULL);
}

bool
memtx_sort_data_reader_begin(struct memtx_sort_data_reader *msdr,
			     uint32_t space_id)
{
	struct memtx_sort_data *msd = msdr->msd;
	struct memtx_sort_data_key key = {space_id, 0};
	mh_int_t i = mh_memtx_sort_data_entries_find(msd->entries, key, NULL);
	if (i == mh_end(msd->entries))
		return false; /* No sort data or it's corrupted. */

	/* Set the PK entry as current one to serve as a space_id holder. */
	struct memtx_sort_data_entry *entry =
		mh_memtx_sort_data_entries_node(msd->entries, i);
	msd->curr_entry = entry;
	return true;
}

bool
memtx_sort_data_seek_index(struct memtx_sort_data_reader *msdr,
			   uint32_t index_id)
{
	struct memtx_sort_data *msd = msdr->msd;
	if (msd->curr_entry == NULL)
		return false; /* The sort data is found to be corrupted. */

	assert(index_id != 0);
	uint32_t space_id = msd->curr_entry->key.space_id;
	struct memtx_sort_data_key key = {space_id, index_id};
	mh_int_t i = mh_memtx_sort_data_entries_find(msd->entries, key, NULL);
	if (i == mh_end(msd->entries))
		return false; /* No SK sort data. */

	struct memtx_sort_data_entry *entry =
		mh_memtx_sort_data_entries_node(msd->entries, i);
	if (fseek(msd->fp, entry->offset, SEEK_SET) != 0) {
		/* The sort data is corrupted, clear all the info. */
		say_error("%s: space %u: SK seek failed, file ignored",
			  msd->fname, msd->curr_entry->key.space_id);
		memtx_sort_data_reader_invalidate(msdr);
		return false;
	}
	msd->curr_entry = entry;
	return true;
}

size_t
memtx_sort_data_size(struct memtx_sort_data_reader *msdr)
{
	struct memtx_sort_data *msd = msdr->msd;
	assert(msd->curr_entry != NULL); /* Only called if sort data exists. */
	return msd->curr_entry->psize;
}

int
memtx_sort_data_read(struct memtx_sort_data_reader *msdr, void *buffer)
{
	struct memtx_sort_data *msd = msdr->msd;
	assert(msd->curr_entry != NULL); /* Only called if sort data exists. */
	return fread(buffer, msd->curr_entry->psize, 1, msd->fp) == 1 ? 0 : -1;
}

struct tuple *
memtx_sort_data_resolve_tuple(struct memtx_sort_data_reader *msdr,
			      struct tuple *old_ptr)
{
	struct memtx_sort_data *msd = msdr->msd;
	assert(msd->curr_entry != NULL); /* Only called if sort data exists. */
	mh_int_t i = mh_ptrptr_find(msdr->old2new, old_ptr, NULL);
	if (i == mh_end(msdr->old2new)) {
		/* The sort data is corrupted, clear all the info. */
		say_error("%s: space %u: tuple resolve failed, file ignored",
			  msd->fname, msd->curr_entry->key.space_id);
		memtx_sort_data_reader_invalidate(msdr);
		return NULL;
	}
	return mh_ptrptr_node(msdr->old2new, i)->val;
}

int
memtx_sort_data_reader_commit(struct memtx_sort_data_reader *msdr)
{
	if (msdr->msd->curr_entry == NULL)
		return -1; /* The sort data is found to be corrupted. */
	return 0;
}

void
memtx_sort_data_reader_delete(struct memtx_sort_data_reader *msdr)
{
	struct memtx_sort_data *msd = msdr->msd;
	fclose(msd->fp);
	msd->fp = NULL;
	memtx_sort_data_delete(msd);
	mh_ptrptr_delete(msdr->old2new);
	free(msdr->buffer);
	free(msdr);
}

/* }}} */
