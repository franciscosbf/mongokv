#include "bson/bson.h"
#include "mongoc/mongoc.h"
#include "postgres.h"
#include "fmgr.h"
#include "utils/elog.h"
#include "utils/hsearch.h"
#include "utils/builtins.h"
#include <stdbool.h>
#include <stddef.h>
#include <string.h>

PG_MODULE_MAGIC;

#define MAX_COLLECTION_NAME 12
#define INITIAL_COLLECTIONS_CACHE_SZ 10

typedef char collection_key[MAX_COLLECTION_NAME + 1 /* '\0' */];
typedef struct {
  mongoc_collection_t *collection;
} collection_entry;

void cleanup_connection(void);
void init_collections_cache(void);
void cleanup_collections_cache(void);
void cleanup(void);
void check_client(void);
void to_collection_entry(const char *collection_name, collection_key *key);
void create_collection(const collection_key *key,
                       collection_entry *entry);
mongoc_collection_t *fetch_collection(const char* collection_name);

static mongoc_client_t *client = NULL;
static mongoc_uri_t *uri = NULL;
static HTAB *collections_cache = NULL;

void cleanup_connection(void) {
  mongoc_client_destroy(client);
  client = NULL;

  mongoc_uri_destroy(uri);
  uri = NULL;
}

void init_collections_cache(void) {
  HASHCTL info = {
    .keysize = sizeof(collection_key),
    .entrysize = sizeof(collection_entry),
  };

  collections_cache = hash_create("collections_cache", INITIAL_COLLECTIONS_CACHE_SZ,
                                  &info, HASH_ELEM & HASH_BLOBS);
}

void cleanup_collections_cache(void) {
  collection_entry *entry = NULL;
  HASH_SEQ_STATUS status;

  if (!collections_cache)
    return;

  hash_seq_init(&status, collections_cache);
  while ((entry = hash_seq_search(&status)) != NULL)
    mongoc_collection_destroy(entry->collection);

  hash_destroy(collections_cache);
  collections_cache = NULL;
}

void cleanup(void) {
  cleanup_connection();

  cleanup_collections_cache();
}

void check_client(void) {
  if (!client)
    elog(ERROR, "the client isn't created");
}

void to_collection_entry(const char *collection_name, collection_key *key) {
  size_t sz = strlen(collection_name);

  if (sz == 0)
    elog(ERROR, "collection name can't be empty");

  if (sz > MAX_COLLECTION_NAME)
    elog(ERROR, "collection name exceeds allowed max: %d characters", MAX_COLLECTION_NAME);

  *(char*)mempcpy(*key, collection_name, sz) = '\0';
}

void create_collection(const collection_key *key,
                       collection_entry *entry) {
  const char *database_name = NULL;
  bson_t *index_key = NULL;
  mongoc_index_model_t *index_model = NULL;
  bson_error_t error;
  bool success;
  bool found;

  database_name = mongoc_uri_get_database(uri);
  entry->collection = mongoc_client_get_collection(client, database_name, *key);

  index_key = BCON_NEW("key", BCON_INT64(1));
  index_model = mongoc_index_model_new(index_key, NULL);
  success = mongoc_collection_create_indexes_with_opts(entry->collection, &index_model,
                                                        1, NULL, NULL, &error);
  mongoc_index_model_destroy(index_model);
  bson_destroy(index_key);
  if (!success) {
    mongoc_collection_destroy(entry->collection);

    hash_search(collections_cache, &key, HASH_REMOVE, &found);

    elog(ERROR, "failed to create index for collection: %s", error.message);
  }
}

mongoc_collection_t *fetch_collection(const char* collection_name) {
  collection_key key = {0};
  bool found;
  collection_entry *entry = NULL;

  to_collection_entry(collection_name, &key);

  entry = hash_search(collections_cache, &key, HASH_ENTER, &found);
  if (!found)
    create_collection(&key, entry);

  return entry->collection;
}

#define UPSERT(COLLECTION_NAME_TEXT, KEY_TEXT, VALUE_BCON) \
  char* collection_name_cstring = NULL; \
  mongoc_collection_t *collection = NULL; \
  char* key_cstring = NULL; \
  bson_t *selector = NULL; \
  bson_t *update = NULL; \
  bson_t *opts = NULL; \
  bool success; \
  bson_error_t error; \
  \
  collection_name_cstring = text_to_cstring(COLLECTION_NAME_TEXT); \
  collection = fetch_collection(collection_name_cstring); \
  \
  key_cstring = text_to_cstring(KEY_TEXT); \
  \
  selector = BCON_NEW("key", BCON_UTF8(key_cstring)); \
  update = BCON_NEW("$set", "{", "key", BCON_UTF8(key_cstring), "value", VALUE_BCON, "}"); \
  opts = BCON_NEW("upsert", BCON_BOOL(true)); \
  success = mongoc_collection_update_one(collection, selector, update, opts, NULL, &error); \
  bson_destroy(selector); \
  bson_destroy(update); \
  bson_destroy(opts); \
  if (!success) { \
    elog(ERROR, "failed to put value: %s", error.message); \
  }

#define GET(COLLECTION_NAME_TEXT, \
            KEY_TEXT, \
            BSON_ITER_HOLDS, \
            BSON_ITER_PARSE, \
            HOLDER) \
  char* collection_name_cstring = NULL; \
  mongoc_collection_t *collection = NULL; \
  char* key_cstring = NULL; \
  bson_t *filter = NULL; \
  bool found; \
  bool with_error; \
  mongoc_cursor_t *cursor; \
  bson_t *doc; \
  bson_error_t error; \
  bson_iter_t iter; \
  const char *error_message = NULL; \
  \
  collection_name_cstring = text_to_cstring(COLLECTION_NAME_TEXT); \
  collection = fetch_collection(collection_name_cstring); \
  \
  key_cstring = text_to_cstring(KEY_TEXT); \
  \
  filter = BCON_NEW("key", BCON_UTF8(key_cstring)); \
  cursor = mongoc_collection_find_with_opts(collection, filter, NULL, NULL); \
  bson_destroy(filter); \
  found = mongoc_cursor_next(cursor, (const bson_t **)&doc); \
  with_error = mongoc_cursor_error(cursor, &error); \
  mongoc_cursor_destroy(cursor); \
  if (!found) { \
    if (with_error) \
      elog(ERROR, "failed to find key: %s", error.message); \
    else \
      elog(ERROR, "key doesn't exist"); \
  } \
  \
  if (!bson_iter_init_find(&iter, doc, "value")) { \
    bson_destroy(doc); \
    elog(ERROR, "value field is somehow missing in pair"); \
  } \
  if (!BSON_ITER_HOLDS(&iter)) { \
    bson_destroy(doc); \
    elog(ERROR, "key doesn't hold value of expected type"); \
  } \
  HOLDER = BSON_ITER_PARSE(&iter); \
  bson_destroy(doc);

void _PG_init(void) {
  mongoc_init();
}

void _PG_fini(void) {
  cleanup();

  mongoc_cleanup();
}

PG_FUNCTION_INFO_V1(create_client);

Datum create_client(PG_FUNCTION_ARGS) {
  text *dns = NULL;
  char *dns_cstring;
  bson_error_t error;
  const char *database_name = NULL;
  bson_t *ping_command = NULL;
  int successful_ping;

  dns = PG_GETARG_TEXT_PP(0);

  if (client)
    elog(ERROR, "client is already created");

  dns_cstring = text_to_cstring(dns);

  if ((uri = mongoc_uri_new_with_error(dns_cstring, &error)) == NULL) {
    cleanup_connection();
    elog(ERROR, "failed to parse connection uri: %s", error.message);
  }

  if ((database_name = mongoc_uri_get_database(uri)) == NULL) {
    cleanup_connection();
    elog(ERROR, "uri doesn't have database");
  }

  if ((client = mongoc_client_new_from_uri_with_error(uri, &error)) == NULL) {
    cleanup_connection();
    elog(ERROR, "failed to create client: %s", error.message);
  }

  ping_command = BCON_NEW("ping", BCON_INT32(1));
  successful_ping = mongoc_client_command_simple(client, database_name,
                                                 ping_command, NULL,
                                                 NULL, &error);
  bson_destroy(ping_command);
  if (!successful_ping) {
    cleanup_connection();
    elog(ERROR, "failed to check connection with database: %s", error.message);
  }

  mongoc_client_set_error_api(client, MONGOC_ERROR_API_VERSION_2);

  init_collections_cache();

  elog(INFO, "client has been created");

  PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(destroy_client);

Datum destroy_client(PG_FUNCTION_ARGS) {
  check_client();

  cleanup();

  elog(INFO, "client was destroyed");

  PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(put_int8);

Datum put_int8(PG_FUNCTION_ARGS) {
  text* collection_name = NULL;
  text* key = NULL;
  int64 value;

  check_client();

  collection_name = PG_GETARG_TEXT_PP(0);
  key = PG_GETARG_TEXT_PP(1);
  value = PG_GETARG_INT64(2);

  UPSERT(collection_name, key, BCON_INT64(value));

  elog(INFO, "int8 stored with success");

  PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(get_int8);

Datum get_int8(PG_FUNCTION_ARGS) {
  text* collection_name = NULL;
  text* key = NULL;
  int64 value;

  check_client();

  collection_name = PG_GETARG_TEXT_PP(0);
  key = PG_GETARG_TEXT_PP(1);

  GET(collection_name,
      key,
      BSON_ITER_HOLDS_INT64,
      bson_iter_int64,
      value);

  elog(INFO, "int8 returned with success");

  PG_RETURN_INT64(value);
}
