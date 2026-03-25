#ifndef FLEXQL_H
#define FLEXQL_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flexql_db flexql_db;

typedef int (*flexql_callback)(void* data, int columnCount, char** values, char** columnNames);

#define FLEXQL_OK 0
#define FLEXQL_ERROR 1

int flexql_open(const char* host, int port, flexql_db** db);
int flexql_close(flexql_db* db);
int flexql_exec(flexql_db* db, const char* sql, flexql_callback callback, void* arg, char** errmsg);
void flexql_free(void* ptr);

#ifdef __cplusplus
}
#endif

#endif
