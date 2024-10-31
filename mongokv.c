#include "bson/bson.h"
#include "mongoc/mongoc.h"
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "glib-2.0/glib.h"
#include "utils/elog.h"
#include <stdbool.h>
#include <stddef.h>
#include <string.h>

PG_MODULE_MAGIC;

#define MAX_COLLECTION_NAME 16

void cleanup_connection(void);
void init_collections_cache(void);
gboolean remove_collection(gpointer key, gpointer value, gpointer _);
void cleanup_collections_cache(void);
void cleanup(void);
void check_client(void);
void check_collection_name(const char *name);
void create_collection(const char *key, mongoc_collection_t **collection);
mongoc_collection_t *fetch_collection(const char* name);
text *bson_iter_utf8_to_text(const bson_iter_t *iter);

static mongoc_client_t *client = NULL;
static mongoc_uri_t *uri = NULL;
static GHashTable *collections_cache = NULL;

void cleanup_connection(void) {
  mongoc_client_destroy(client);
  client = NULL;

  mongoc_uri_destroy(uri);
  uri = NULL;
}

void init_collections_cache(void) {
  collections_cache = g_hash_table_new_full(g_str_hash, g_str_equal, NULL, NULL);
}

gboolean remove_collection(gpointer key, gpointer value, gpointer _) {
  char *name;
  mongoc_collection_t *collection;

  name = key;
  collection = value;

  pfree(name);
  mongoc_collection_destroy(collection);

  return TRUE;
}

void cleanup_collections_cache(void) {
  if (!collections_cache)
    return;

  g_hash_table_foreach_remove(collections_cache, remove_collection, NULL);
  g_hash_table_destroy(collections_cache);
  collections_cache = NULL;
}

void cleanup(void) {
  cleanup_connection();

  cleanup_collections_cache();
}

void check_client(void) {
  if (!client)
    elog(ERROR, "client isn't initialized");
}

void check_collection_name(const char *name) {
  size_t sz;
  char *current;

  current = (char *)name;
  while ((sz = current - name) < MAX_COLLECTION_NAME && *current != '\0')
    current++;

  switch (sz) {
    case 0: case MAX_COLLECTION_NAME:
      pfree((char *)name);
      elog(ERROR, "collection name must be non empty and with %d characters max",
          MAX_COLLECTION_NAME);
  }
}

void create_collection(const char* name, mongoc_collection_t **collection) {
  const char *database_name = NULL;
  bson_t *index_key = NULL;
  mongoc_index_model_t *index_model = NULL;
  bson_error_t error;
  bool success;
  Size name_sz;
  char *_name;

  database_name = mongoc_uri_get_database(uri);
  *collection = mongoc_client_get_collection(client, database_name, name);

  index_key = BCON_NEW("key", BCON_INT64(1));
  index_model = mongoc_index_model_new(index_key, NULL);
  success = mongoc_collection_create_indexes_with_opts(*collection, &index_model,
                                                       1, NULL, NULL, &error);
  mongoc_index_model_destroy(index_model);
  bson_destroy(index_key);
  if (!success) {
    mongoc_collection_destroy(*collection);
    pfree((char *)name);
    elog(ERROR, "failed to create index for collection: %s", error.message);
  }

  name_sz = strlen(name) + 1;
  _name = palloc(name_sz);
  *(char *)mempcpy(_name, name, name_sz) = '\0';
  g_hash_table_insert(collections_cache, _name, *collection);
}

mongoc_collection_t *fetch_collection(const char* name) {
  mongoc_collection_t *collection;

  check_collection_name(name);

  if ((collection = g_hash_table_lookup(collections_cache, name)) == NULL)
    create_collection(name, &collection);

  return collection;
}

text *bson_iter_utf8_to_text(const bson_iter_t *iter) {
  const char *value;

  value = bson_iter_utf8(iter, 0);

  return cstring_to_text(value);
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
  pfree(collection_name_cstring); \
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
  pfree(key_cstring); \
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
  \
  collection_name_cstring = text_to_cstring(COLLECTION_NAME_TEXT); \
  collection = fetch_collection(collection_name_cstring); \
  pfree(collection_name_cstring); \
  \
  key_cstring = text_to_cstring(KEY_TEXT); \
  \
  filter = BCON_NEW("key", BCON_UTF8(key_cstring)); \
  cursor = mongoc_collection_find_with_opts(collection, filter, NULL, NULL); \
  bson_destroy(filter); \
  pfree(key_cstring); \
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

PG_FUNCTION_INFO_V1(put_text);

Datum put_text(PG_FUNCTION_ARGS) {
  text *collection_name = NULL;
  text *key = NULL;
  text *value;
  char *value_cstring;

  check_client();

  collection_name = PG_GETARG_TEXT_PP(0);
  key = PG_GETARG_TEXT_PP(1);
  value = PG_GETARG_TEXT_PP(2);

  value_cstring = text_to_cstring(value);
  UPSERT(collection_name, key, BCON_UTF8(value_cstring));

  elog(INFO, "text stored with success");

  PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(get_text);

Datum get_text(PG_FUNCTION_ARGS) {
  text *collection_name = NULL;
  text *key = NULL;
  text *value;

  check_client();

  collection_name = PG_GETARG_TEXT_PP(0);
  key = PG_GETARG_TEXT_PP(1);

  GET(collection_name,
      key,
      BSON_ITER_HOLDS_UTF8,
      bson_iter_utf8_to_text,
      value);

  elog(INFO, "text returned with success");

  PG_RETURN_TEXT_P(value);
}
