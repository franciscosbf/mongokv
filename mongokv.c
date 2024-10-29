#include "bson/bson.h"
#include "mongoc/mongoc.h"
#include "postgres.h"
#include "fmgr.h"
#include "utils/elog.h"
#include <stdbool.h>

mongoc_client_t *client = NULL;
mongoc_uri_t *uri = NULL;

void cleanup() {
  mongoc_client_destroy(client);
  client = NULL;

  mongoc_uri_destroy(uri);
  uri = NULL;
}

PG_MODULE_MAGIC;

void _PG_init(void) {
  mongoc_init();

  elog(INFO, "mongokv extension has been initialized");
}

void _PG_fini(void) {
  cleanup();

  mongoc_cleanup();

  elog(INFO, "mongokv extension has been removed");
}

PG_FUNCTION_INFO_V1(create_client);

Datum create_client(PG_FUNCTION_ARGS) {
  text *dns;
  mongoc_uri_t *uri;
  bson_error_t error;
  const char *database_name;
  bson_t *ping_command;
  int successful_ping;

  dns = PG_GETARG_TEXT_PP(0);

  if (client) elog(ERROR, "client is already created");

  if ((uri = mongoc_uri_new_with_error(dns->vl_dat, &error)) == NULL) {
    cleanup();

    elog(ERROR, "failed to parse connection uri: %s", error.message);
  }

  if ((database_name = mongoc_uri_get_database(uri)) == NULL) {
    cleanup();

    elog(ERROR, "uri doesn't have database");
  }

  if ((client = mongoc_client_new_from_uri_with_error(uri, &error)) == NULL) {
    cleanup();

    elog(ERROR, "failed to create client: %s", error.message);
  }

  ping_command = BCON_NEW("ping", BCON_INT32(1));
  successful_ping = mongoc_client_command_simple(client, database_name,
                                                 ping_command, NULL,
                                                 NULL, &error);
  bson_destroy(ping_command);
  if (!successful_ping) {
    cleanup();

    elog(ERROR, "failed to check connection with database: %s", error.message);
  }

  mongoc_client_set_error_api(client, MONGOC_ERROR_API_VERSION_2);

  elog(INFO, "client has been created");

  PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(destroy_client);

Datum destroy_client(PG_FUNCTION_ARGS) {
  if (!client) elog(ERROR, "the client isn't created");

  cleanup();

  elog(INFO, "client was destroyed");

  PG_RETURN_VOID();
}
