/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
#include "test_util.h"


#define URI_FORMAT "table:test_bug010%d"
#define NUM_TABLES 200

//#define CONN_CONFIG "checkpoint_sync=false"

//#define CONN_CONFIG "log=(recover=on,remove=false)"



static const char table_config[] = "key_format=S,value_format=i";
static void *thread_func_checkpoint(void *);

bool done = false;

/* Structures definition. */
struct thread_data {
    WT_CONNECTION *conn;
};



/*
 * thread_func_checkpoint --
 */
static void *
thread_func_checkpoint(void *arg)
{
    struct thread_data *td;
    WT_SESSION *session;

    td = (struct thread_data *)arg;
    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));

    while (!done)
    {
        // Sleep for 10 milliseconds.
        __wt_sleep(0.01, 0);
        testutil_check(session->checkpoint(session, NULL));
    }

    testutil_check(session->close(session, NULL));
    session = NULL;

    return (NULL);
}


/*
 * main --
 *     Methods implementation.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    WT_SESSION *session;
    WT_CURSOR *cursor;
    WT_CURSOR *cursor_list[NUM_TABLES];
    struct thread_data td;
    pthread_t thread_checkpoint;

    int i, expected_val, table_value;
    char uri[128];

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));

    printf("Work directory: %s\n", opts->home);
    testutil_make_work_dir(opts->home);
    testutil_check(wiredtiger_open(opts->home, NULL, "create,checkpoint_sync=false", &opts->conn));

    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));

    td.conn = opts->conn;

    for (i = 0; i < NUM_TABLES; i++) {
        sprintf(uri, URI_FORMAT, i);
        //printf("Creating table : %s\n", uri);
        /* Create and populate table. Checkpoint the data after that. */
        testutil_check(session->create(session, uri, table_config));

        testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));
        cursor->set_key(cursor, "a");
        cursor->set_value(cursor, 0);
        testutil_check(cursor->insert(cursor));
        cursor->close(cursor);
    }

    testutil_check(session->checkpoint(session, NULL));

    expected_val = 0;

    /* Open cursors. */
    for (i = 0; i < NUM_TABLES; i++) {
        sprintf(uri, URI_FORMAT, i);
        testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor_list[i]));
    }
    for (i = 0; i < 10; ++i) {
        printf("Doing iteration : %d\n", i);
        testutil_check(pthread_create(&thread_checkpoint, NULL, thread_func_checkpoint, &td));

        expected_val += 1;
        for (int j = 0; j < NUM_TABLES; ++j) {
            cursor_list[j]->set_key(cursor_list[j], "a");
            cursor_list[j]->set_value(cursor_list[j],expected_val);
            testutil_check(cursor_list[j]->update(cursor_list[j]));
            //cursor->close(cursor);
        }

        done = true;

        /* Wait for the threads to finish the work. */
        (void)pthread_join(thread_checkpoint, NULL);

        // Execute another checkpoint, to make sure we have a consistent
        testutil_check(session->checkpoint(session, NULL));

        // Data validation
        for (int j = 0; j < NUM_TABLES; ++j) {
            sprintf(uri, URI_FORMAT, j);
            testutil_check(session->open_cursor(session, uri, NULL, "checkpoint=WiredTigerCheckpoint", &cursor));

            cursor->set_key(cursor, "a");
            testutil_check(cursor->next(cursor));
            testutil_check(cursor->get_value(cursor, &table_value));

            //printf("Table Value : %d -- Expected Value : %d\n", table_value, expected_val);

            testutil_assert(table_value == expected_val);
            cursor->close(cursor);
        }
    }

    /* Close cursors. */
    for (i = 0; i < NUM_TABLES; i++) {
        cursor_list[i]->close(cursor_list[i]);
    }
}
