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

/*
 * This program tests skip list ordering under concurrent workloads. It copies some of the skip list
 * code from the btree, but links against the WiredTiger library for all of the support functions.
 *
 * This is a quick and dirty test for WT-10461. If we ever decide to make this a standard part of
 * the csuite, we'll need to refactor things so it uses the same code as WT, rather than a copy of
 * the code.
 */

// TODO
// - Hit the gd bug
// - confirm the copied WT functions actually match WT
// - Update comments
// - expose insert_search/insert to this test? Probs don't need to copy past the functions
// - How to encourage out of order loads??

#include <math.h>
#include "test_util.h"
#include <math.h>

extern int __wt_optind;
extern char *__wt_optarg;

static uint64_t seed = 0;

#define KEY_SIZE 1024

/* Test parameters. Eventually these should become command line args */

#define CHECK_THREADS 3  /* Can change this as needed */
#define INSERT_THREAD 1 /* !!!! We only want 1 insert thread. Don't change this !!!! */
#define INVALIDATE_THREAD 1 /* !!!! We only want 1 invalidate thread. Don't change this !!!! */

typedef struct {
    WT_CONNECTION *conn;
    WT_INSERT_HEAD *ins_head;
    uint32_t id;
} THREAD_DATA;


// test states
// - start up insert threads
// - once inserts are ready, start up checks
// - once checks are running, run inserts
// - once inserts are finished, join checks
// - once checks are joined, join inserts
static volatile int active_check_threads;
static volatile int active_insert_threads;

static void usage(void) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));

/*
 * usage --
 *     Print a usage message.
 */
static void
usage(void)
{
    fprintf(
      stderr, "usage: %s [-adr] [-h dir] [-S seed] [-t insert threads]\n", progname);
    fprintf(stderr, "Only one of the -adr options may be used\n");
    exit(EXIT_FAILURE);
}

/*
 * We don't care about the values we store in our mock insert list. So all entries will point to the
 * dummy update. Likewise, the insert code uses the WT page lock when it needs to exclusive access.
 * We're don't have that, so we just set up a single global spinlock that all threads use since
 * they're all operating on the same skiplist.
 */
static WT_UPDATE dummy_update;

static WT_SPINLOCK page_lock;

/*
 * search_insert --
 *     Find the location for an insert into the skip list. Based o __wt_search_insert()
 */
static int
search_insert(
  WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_INSERT_HEAD *ins_head, WT_ITEM *srch_key)
{
    WT_INSERT *ins, **insp, *last_ins;
    WT_ITEM key;
    size_t match, skiphigh, skiplow;
    int cmp, i;

    cmp = 0; /* -Wuninitialized */

    /*
     * The insert list is a skip list: start at the highest skip level, then go as far as possible
     * at each level before stepping down to the next.
     */
    match = skiphigh = skiplow = 0;
    ins = last_ins = NULL;
    for (i = WT_SKIP_MAXDEPTH - 1, insp = &ins_head->head[i]; i >= 0;) {
        if ((ins = *insp) == NULL) {
        // This should fix the bug once it fires:
        // WT_ORDERED_READ(ins, *insp);
        // if (ins == NULL) {
            cbt->next_stack[i] = NULL;
            cbt->ins_stack[i--] = insp--;
            continue;
        }

        /*
         * Comparisons may be repeated as we drop down skiplist levels; don't repeat comparisons,
         * they might be expensive.
         */
        if (ins != last_ins) {
            last_ins = ins;
            key.data = WT_INSERT_KEY(ins);
            key.size = WT_INSERT_KEY_SIZE(ins);
            match = WT_MIN(skiplow, skiphigh);

            WT_RET(__wt_compare_skip(session, NULL, srch_key, &key, &cmp, &match));
        }

        if (cmp > 0) { /* Keep going at this level */
            insp = &ins->next[i];
            WT_ASSERT(session, match >= skiplow);
            skiplow = match;
        } else if (cmp < 0) { /* Drop down a level */
            cbt->next_stack[i] = ins;
            cbt->ins_stack[i--] = insp--;
            WT_ASSERT(session, match >= skiphigh);
            skiphigh = match;
        } else
            for (; i >= 0; i--) {
                cbt->next_stack[i] = ins->next[i];
                cbt->ins_stack[i] = &ins->next[i];
            }
    }

    /*
     * For every insert element we review, we're getting closer to a better choice; update the
     * compare field to its new value. If we went past the last item in the list, return the last
     * one: that is used to decide whether we are positioned in a skiplist.
     */
    cbt->compare = -cmp;
    cbt->ins = (ins != NULL) ? ins : last_ins;
    cbt->ins_head = ins_head;
    return (0);
}

/*
 * insert_simple_func --
 *     Add a WT_INSERT entry to the middle of a skiplist. Copy of __insert_simple_func().
 */
static inline int
insert_simple_func(
  WT_SESSION_IMPL *session, WT_INSERT ***ins_stack, WT_INSERT *new_ins, u_int skipdepth)
{
    u_int i;

    WT_UNUSED(session);

    /*
     * Update the skiplist elements referencing the new WT_INSERT item. If we fail connecting one of
     * the upper levels in the skiplist, return success: the levels we updated are correct and
     * sufficient. Even though we don't get the benefit of the memory we allocated, we can't roll
     * back.
     *
     * All structure setup must be flushed before the structure is entered into the list. We need a
     * write barrier here, our callers depend on it. Don't pass complex arguments to the macro, some
     * implementations read the old value multiple times.
     */
    for (i = 0; i < skipdepth; i++) {
        WT_INSERT *old_ins = *ins_stack[i];
        if (old_ins != new_ins->next[i] || !__wt_atomic_cas_ptr(ins_stack[i], old_ins, new_ins))
            return (i == 0 ? WT_RESTART : 0);
    }

    return (0);
}

/*
 * insert_serial_func --
 *     Add a WT_INSERT entry to a skiplist. Copy of __insert_serial_func()
 */
static inline int
insert_serial_func(WT_SESSION_IMPL *session, WT_INSERT_HEAD *ins_head, WT_INSERT ***ins_stack,
  WT_INSERT *new_ins, u_int skipdepth)
{
    u_int i;

    /* The cursor should be positioned. */
    WT_ASSERT(session, ins_stack[0] != NULL);

    /*
     * Update the skiplist elements referencing the new WT_INSERT item.
     *
     * Confirm we are still in the expected position, and no item has been added where our insert
     * belongs. If we fail connecting one of the upper levels in the skiplist, return success: the
     * levels we updated are correct and sufficient. Even though we don't get the benefit of the
     * memory we allocated, we can't roll back.
     *
     * All structure setup must be flushed before the structure is entered into the list. We need a
     * write barrier here, our callers depend on it. Don't pass complex arguments to the macro, some
     * implementations read the old value multiple times.
     */
    for (i = 0; i < skipdepth; i++) {
        WT_INSERT *old_ins = *ins_stack[i];
        if (old_ins != new_ins->next[i] || !__wt_atomic_cas_ptr(ins_stack[i], old_ins, new_ins))
            return (i == 0 ? WT_RESTART : 0);
        if (ins_head->tail[i] == NULL || ins_stack[i] == &ins_head->tail[i]->next[i])
            ins_head->tail[i] = new_ins;
    }

    return (0);
}

/*
 * insert_serial --
 *     Top level function for inserting a WT_INSERT into a skiplist. Based on __wt_insert_serial()
 */
static inline int
insert_serial(WT_SESSION_IMPL *session, WT_INSERT_HEAD *ins_head, WT_INSERT ***ins_stack,
  WT_INSERT **new_insp, u_int skipdepth)
{
    WT_DECL_RET;
    WT_INSERT *new_ins;
    u_int i;
    bool simple;

    /* Clear references to memory we now own and must free on error. */
    new_ins = *new_insp;
    *new_insp = NULL;

    simple = true;
    for (i = 0; i < skipdepth; i++)
        if (new_ins->next[i] == NULL)
            simple = false;

    if (simple)
        ret = insert_simple_func(session, ins_stack, new_ins, skipdepth);
    else {
        __wt_spin_lock(session, &page_lock);
        ret = insert_serial_func(session, ins_head, ins_stack, new_ins, skipdepth);
        __wt_spin_unlock(session, &page_lock);
    }

    if (ret != 0) {
        /* Free unused memory on error. */
        __wt_free(session, new_ins);
        return (ret);
    }

    return (0);
}

/*
 * row_insert --
 *     Our version of the __wt_row_modify() function, with everything stripped out except for the
 *     relevant insert path.
 */
static int
row_insert(WT_CURSOR_BTREE *cbt, const WT_ITEM *key, WT_INSERT_HEAD *ins_head)
{
    WT_DECL_RET;
    WT_INSERT *ins;
    WT_SESSION_IMPL *session;
    size_t ins_size;
    u_int i, skipdepth;

    ins = NULL;
    session = CUR2S(cbt);

    /*
     * Allocate the insert array as necessary.
     *
     * We allocate an additional insert array slot for insert keys sorting less than any key on the
     * page. The test to select that slot is baroque: if the search returned the first page slot, we
     * didn't end up processing an insert list, and the comparison value indicates the search key
     * was smaller than the returned slot, then we're using the smallest-key insert slot. That's
     * hard, so we set a flag.
     */

    /* Choose a skiplist depth for this insert. */
    skipdepth = __wt_skip_choose_depth(session);

    /*
     * Allocate a WT_INSERT/WT_UPDATE pair and transaction ID, and update the cursor to reference it
     * (the WT_INSERT_HEAD might be allocated, the WT_INSERT was allocated).
     */
    WT_ERR(__wt_row_insert_alloc(session, key, skipdepth, &ins, &ins_size));
    cbt->ins_head = ins_head;
    cbt->ins = ins;

    ins->upd = &dummy_update;
    ins_size += WT_UPDATE_SIZE;

    /*
     * If there was no insert list during the search, the cursor's information cannot be correct,
     * search couldn't have initialized it.
     *
     * Otherwise, point the new WT_INSERT item's skiplist to the next elements in the insert list
     * (which we will check are still valid inside the serialization function).
     *
     * The serial mutex acts as our memory barrier to flush these writes before inserting them into
     * the list.
     */
    if (cbt->ins_stack[0] == NULL)
        for (i = 0; i < skipdepth; i++) {
            cbt->ins_stack[i] = &ins_head->head[i];
            ins->next[i] = cbt->next_stack[i] = NULL;
        }
    else
        for (i = 0; i < skipdepth; i++)
            ins->next[i] = cbt->next_stack[i];

    /* Insert the WT_INSERT structure. */
    WT_ERR(insert_serial(session, cbt->ins_head, cbt->ins_stack, &ins, skipdepth));

err:
    return (ret);
}

/*
 * insert --
 *     Test function that inserts a new entry with the given key string into our skiplist.
 */
static int
insert(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_INSERT_HEAD *ins_head, char *key)
{
    WT_ITEM new;

    new.data = key;
    /* Include the terminal nul character in the key for easier printing. */
    new.size = strlen(key) + 1;
    WT_RET(search_insert(session, cbt, ins_head, &new));
    WT_RET(row_insert(cbt, &new, ins_head));

    return (0);
}

/*
 * thread_insert_run --
 *     An insert thread. Continually insert keys in decreasing order. 
 *     These keys are intentionally chosen such that each newly inserted 
 *     key has a longer matching prefix with our search key in check_run. 
 */
static WT_THREAD_RET
thread_insert_run(void *arg)
{
    WT_CONNECTION *conn;
    WT_CURSOR_BTREE *cbt;
    WT_INSERT_HEAD *ins_head;
    WT_SESSION *session;
    THREAD_DATA *td;
    uint32_t i;
    char **key_list;

    td = (THREAD_DATA *)arg;
    conn = td->conn;
    ins_head = td->ins_head;

    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /* Set up state as if we have a btree that is accessing an insert list. */
    cbt = dcalloc(1, sizeof(WT_CURSOR_BTREE));
    ((WT_CURSOR *)cbt)->session = session;

    // We're assuming only one INSERT THREAD in use here.
    // It'll take responsibility for setting up the initial state

    /*
     * Generate the keys.
     * N.B., the key strings here are stored in the skip list. So we need a separate buffer for each
     * key.
     */
    // This generates:
    // 1
    // 01
    // 001
    // 0001
    // 00001
    // up to KEY_SIZE chars long (incl. nul byte)
    // TODO - hacked in bookends
    // TODO - index 0 is nul!!
    key_list = dmalloc(((KEY_SIZE - 1) + 2) * sizeof(char *));
    for (i = 1; i < (KEY_SIZE - 1); i++) {
        key_list[i] = dmalloc(KEY_SIZE);
        sprintf(key_list[i], "%0*d", (int)i, 1);
    }

    #define LEFT_BOOKEND KEY_SIZE - 1
    key_list[LEFT_BOOKEND] = dmalloc(KEY_SIZE);
    // KEY_SIZE - 2 zeroes. Our search key is KEY_SIZE -1 zeroes so this is smaller.
    sprintf(key_list[LEFT_BOOKEND], "%0*d", KEY_SIZE - 2, 0);

    #define RIGHT_BOOKEND KEY_SIZE
    key_list[RIGHT_BOOKEND] = dmalloc(KEY_SIZE);
    sprintf(key_list[RIGHT_BOOKEND], "11111111");

    WT_IGNORE_RET(insert((WT_SESSION_IMPL *)session, cbt, ins_head, key_list[LEFT_BOOKEND]));
    WT_IGNORE_RET(insert((WT_SESSION_IMPL *)session, cbt, ins_head, key_list[RIGHT_BOOKEND]));

    __atomic_fetch_add(&active_insert_threads, 1, __ATOMIC_SEQ_CST);
    while ( active_check_threads != (CHECK_THREADS + INVALIDATE_THREAD))
        ;

    /* Insert the keys. */
    for (i = 1; i < 63; i++) {
        WT_IGNORE_RET(insert((WT_SESSION_IMPL *)session, cbt, ins_head, key_list[i]));
    }

    // printf("end decr\n");
    __atomic_fetch_sub(&active_insert_threads, 1, __ATOMIC_SEQ_CST);

    // Wait till all checks are joined so we don't free the skiplist under them
    while (active_check_threads != 0)
        ;   

    // TODO - We're leaking memory somewhere... For now handle it by running 
    // the binary multiple times for a shorter time in evergreen.yml
    free(cbt);
    for (i = 1; i < 63; i++) {
        free(key_list[i]);
    }
    free(key_list);

    return (WT_THREAD_RET_VALUE);
}

/*
 * thread_invalidate_run --
 *     TODO - Continually update the level 8 pointer to force a read-invalidate for all other CPUs.
 *     If we're lucky this triggers out of order reads in the check threads.
 */
static WT_THREAD_RET
thread_invalidate_run(void *arg)
{
    WT_CONNECTION *conn;
    WT_CURSOR_BTREE *cbt;
    WT_INSERT_HEAD *ins_head;
    WT_SESSION *session;
    THREAD_DATA *td;

    WT_ITEM srch_key;
    WT_DECL_RET;

    // Stolen from insert_search
    WT_INSERT *ins, **insp, *last_ins;
    WT_ITEM key;
    size_t match, skiphigh, skiplow;
    int cmp, i;

    // Thread setup
    td = (THREAD_DATA *)arg;
    conn = td->conn;
    ins_head = td->ins_head;

    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /* Set up state as if we have a btree that is accessing an insert list. */
    cbt = dcalloc(1, sizeof(WT_CURSOR_BTREE));
    ((WT_CURSOR *)cbt)->session = session;

    // Wait for active threads
    while(active_insert_threads != INSERT_THREAD)
        ;

    //////////////////////////////////////////////////
    // Find correct ptr - stolen from search_insert //
    //////////////////////////////////////////////////

    srch_key.data = dmalloc(KEY_SIZE);
    sprintf((char*)srch_key.data, "11111111");
    srch_key.size = 9;

    cmp = -2; /* -Wuninitialized */

    /*
     * The insert list is a skip list: start at the highest skip level, then go as far as possible
     * at each level before stepping down to the next.
     */
    match = skiphigh = skiplow = 0;
    ins = last_ins = NULL;

    // hardcode to 8 
    // Why 8? All these pointers are in an array and each ptr is 8 bytes, so we can fit 8 
    // to a 64 byte cache line. The CAS should invalidate the cache line of levels 8-15 
    // while leaving levels 0-7 untouched.
    // !!!!!!!!!!!
    // THIS ASSUMES SKIPLIST ALWAYS HAS 8+ LEVELS !!!!!
    // !!!!!!!!!!!
    i = 8;
    insp = &ins_head->head[i];
    while (cmp != 0) {
        ins = *insp;

        // If ins is NULL we've seen the end of the skiplist. 
        // That shouldn't happen with current setup (16 levels, always populated).
        WT_ASSERT((WT_SESSION_IMPL*)session, ins != NULL);

        if (ins != last_ins) {
            last_ins = ins;
            key.data = WT_INSERT_KEY(ins);
            key.size = WT_INSERT_KEY_SIZE(ins);

            // Ignore match here. We just want to find the correct key to invalidate.
            WT_UNUSED(match);

            ret = __wt_compare_skip((WT_SESSION_IMPL*)session, NULL, &srch_key, &key, &cmp, &match);
            if(ret != 0) {
                printf("COMPARE FAIL IN INVALIDATE_RUN\n");
                exit(1);
            }
        }

        if (cmp > 0) { /* Keep going at this level */
            insp = &ins->next[i];
            WT_ASSERT((WT_SESSION_IMPL*)session, match >= skiplow);
            skiplow = match;
        } else if (cmp < 0) { /* Drop down a level */
            // We shouldn't need to drop a level with this test setup.
            // 16 levels, always filled as per above and we're searching for a key we know is present.
            WT_ASSERT((WT_SESSION_IMPL*)session, false);
        } else {
            __atomic_fetch_add(&active_check_threads, 1, __ATOMIC_SEQ_CST);
            while(active_insert_threads != INSERT_THREAD)
                ;

            // Continually update the next pointer to ins, but always with its existing value.
            // This doesn't change anything, but forces a read-invalidate for all other threads.
            while(active_insert_threads != 0) {
                __wt_atomic_cas_ptr(&last_ins->next[8], last_ins->next[8], last_ins->next[8]);
            }

            return (WT_THREAD_RET_VALUE);
        }
    } 

    // Should be unreachable
    WT_ASSERT((WT_SESSION_IMPL*)session, false);
    return (WT_THREAD_RET_VALUE);
}

/*
 * thread_check_run --
 *     A check thread sits in a loop running search_insert for KEY_SIZE - 1 zeroes
 *     Note that this never inserts the key, just searches for it. If there's 
 *     an out of order read we'll catch it in the match >= skiphigh assert in search_insert.
 */
static WT_THREAD_RET
thread_check_run(void *arg)
{
    WT_CONNECTION *conn;
    WT_INSERT_HEAD *ins_head;
    WT_SESSION *session;
    THREAD_DATA *td;
    WT_ITEM check_key;
    WT_CURSOR_BTREE *cbt;

    td = (THREAD_DATA *)arg;
    conn = td->conn;
    ins_head = td->ins_head;

    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /* Set up state as if we have a btree that is accessing an insert list. */
    cbt = dcalloc(1, sizeof(WT_CURSOR_BTREE));
    ((WT_CURSOR *)cbt)->session = session;
    
    // Set up our search key. It'll always be just after LEFT_BOOKEND in the skip list
    check_key.data = dmalloc(KEY_SIZE);
    sprintf((char*)check_key.data, "%0*d", KEY_SIZE - 1, 0);
    /* Include the terminal nul character in the key for easier printing. */
    check_key.size = KEY_SIZE;

    while(active_insert_threads != INSERT_THREAD)
        ;
    __atomic_fetch_add(&active_check_threads, 1, __ATOMIC_SEQ_CST);

    /* Keep checking the skip list until the insert load has finished */
    while (active_insert_threads != 0)
        WT_IGNORE_RET(search_insert((WT_SESSION_IMPL *)session, cbt, ins_head, &check_key));

    __atomic_fetch_sub(&active_check_threads, 1, __ATOMIC_SEQ_CST);

    return (WT_THREAD_RET_VALUE);
}

static int run(const char *working_dir) 
{
    char command[1024], home[1024];
    int status;
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_INSERT_HEAD *ins_head;
    THREAD_DATA *td;
    wt_thread_t *thr;
    uint32_t nthreads, i;
    
    nthreads = CHECK_THREADS + INVALIDATE_THREAD + INSERT_THREAD;

    testutil_work_dir_from_path(home, sizeof(home), working_dir);
    testutil_check(__wt_snprintf(command, sizeof(command), "rm -rf %s; mkdir %s", home, home));
    if ((status = system(command)) < 0)
        testutil_die(status, "system: %s", command);

    testutil_check(wiredtiger_open(home, NULL, "create", &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(__wt_spin_init((WT_SESSION_IMPL *)session, &page_lock, "fake page lock"));
    ins_head = dcalloc(1, sizeof(WT_INSERT_HEAD));

    /* Set up threads */
    td = dmalloc(nthreads * sizeof(THREAD_DATA));
    for (i = 0; i < nthreads; i++) {
        td[i].conn = conn;
        td[i].id = i;
        td[i].ins_head = ins_head;
    }

    thr = dmalloc(nthreads * sizeof(wt_thread_t));

    /* Start threads */
    active_check_threads = 0;
    active_insert_threads = 0;
    for (i = 0; i < nthreads; i++)
        if (i < CHECK_THREADS)
            testutil_check(__wt_thread_create(NULL, &thr[i], thread_check_run, &td[i]));
        else if(i < CHECK_THREADS + INVALIDATE_THREAD)
            testutil_check(__wt_thread_create(NULL, &thr[i], thread_invalidate_run, &td[i]));    
        else
            testutil_check(__wt_thread_create(NULL, &thr[i], thread_insert_run, &td[i]));

    /* Wait check threads to stop */
    for (i = 0; i < CHECK_THREADS; i++)
        testutil_check(__wt_thread_join(NULL, &thr[i]));

    /* Wait for invalidate thread to complete */
    for (i = CHECK_THREADS; i < CHECK_THREADS + INVALIDATE_THREAD; i++)
        testutil_check(__wt_thread_join(NULL, &thr[i]));

    /* Wait for insert thread to complete */
    for (i = CHECK_THREADS + INVALIDATE_THREAD; i < CHECK_THREADS + INSERT_THREAD; i++)
        testutil_check(__wt_thread_join(NULL, &thr[i]));
    
    testutil_check(conn->close(conn, ""));

    // printf("Success.\n");
    testutil_clean_test_artifacts(home);
    testutil_clean_work_dir(home);

    return (EXIT_SUCCESS);
}

/*
 * main --
 *     Test body
 */
int
main(int argc, char *argv[])
{
    WT_RAND_STATE rnd;
    const char *working_dir;
    int ch;

    working_dir = "WT_TEST.skip_list_stress";

    while ((ch = __wt_getopt(progname, argc, argv, "h:S:")) != EOF)
        switch (ch) {
        case 'h':
            working_dir = __wt_optarg;
            break;
        case 'S':
            seed = (uint64_t)atoll(__wt_optarg);
            break;
        default:
            usage();
        }
    argc -= __wt_optind;
    if (argc != 0)
        usage();

    if (seed == 0) {
        __wt_random_init_seed(NULL, &rnd);
        seed = rnd.v;
    } else
        rnd.v = seed;

    // 2.5 mins of testing. Run multiple times in evergreen.yml
    for(int j = 0; j < 1000; j++) {
        printf("loop %d\n", j);
        run(working_dir);
        // Cause evergreen buffers output and I'm impatient
        fflush(stdout);
    }
    return 0;
}
