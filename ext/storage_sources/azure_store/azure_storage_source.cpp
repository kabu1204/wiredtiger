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
#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <algorithm>
#include <memory>
#include <mutex>
#include <vector>

#include "azure_connection.h"
#include "wt_internal.h"

struct azure_file_system;
struct azure_file_handle;
struct azure_store {
    WT_STORAGE_SOURCE store;
    WT_EXTENSION_API *wt_api;

    std::mutex fs_mutex;
    std::vector<azure_file_system *> azure_fs;
    uint32_t reference_count;
};

struct azure_file_system {
    WT_FILE_SYSTEM fs;
    azure_store *store;
    WT_FILE_SYSTEM *wt_fs;

    std::mutex fh_mutex;
    std::vector<azure_file_handle> azure_fh;
    std::unique_ptr<azure_connection> azure_conn;
    std::string home_dir;
};

struct azure_file_handle {
    WT_FILE_HANDLE fh;
    azure_store *store;
};

// WT_STORAGE_SOURCE Interface
static int azure_customize_file_system(
  WT_STORAGE_SOURCE *, WT_SESSION *, const char *, const char *, const char *, WT_FILE_SYSTEM **);
static int azure_add_reference(WT_STORAGE_SOURCE *);
static int azure_terminate(WT_STORAGE_SOURCE *, WT_SESSION *);
static int azure_flush(WT_STORAGE_SOURCE *, WT_SESSION *, WT_FILE_SYSTEM *, const char *,
  const char *, const char *) __attribute__((__unused__));
static int azure_flush_finish(WT_STORAGE_SOURCE *, WT_SESSION *, WT_FILE_SYSTEM *, const char *,
  const char *, const char *) __attribute__((__unused__));

// WT_FILE_SYSTEM Interface
static int azure_object_list(WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, char ***,
  uint32_t *) __attribute__((__unused__));
static int azure_object_list_single(WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *,
  char ***, uint32_t *) __attribute__((__unused__));
static int azure_object_list_free(WT_FILE_SYSTEM *, WT_SESSION *, char **, uint32_t)
  __attribute__((__unused__));
static int azure_file_system_terminate(WT_FILE_SYSTEM *, WT_SESSION *);
static int azure_file_exists(WT_FILE_SYSTEM *, WT_SESSION *, const char *, bool *)
  __attribute__((__unused__));
static int azure_remove(WT_FILE_SYSTEM *, WT_SESSION *, const char *, uint32_t)
  __attribute__((__unused__));
static int azure_rename(WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, uint32_t)
  __attribute__((__unused__));
static int azure_object_size(WT_FILE_SYSTEM *, WT_SESSION *, const char *, wt_off_t *)
  __attribute__((__unused__));
static int azure_file_open(WT_FILE_SYSTEM *, WT_SESSION *, const char *, WT_FS_OPEN_FILE_TYPE,
  uint32_t, WT_FILE_HANDLE **) __attribute__((__unused__));

// WT_FILE_HANDLE Interface
static int azure_file_close(WT_FILE_HANDLE *, WT_SESSION *) __attribute__((__unused__));
static int azure_file_lock(WT_FILE_HANDLE *, WT_SESSION *, bool) __attribute__((__unused__));
static int azure_file_read(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t, size_t, void *)
  __attribute__((__unused__));
static int azure_file_size(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t *) __attribute__((__unused__));

// Return a customised file system to access the Azure storage source.
static int
azure_customize_file_system(WT_STORAGE_SOURCE *storage_source, WT_SESSION *session,
  const char *bucket, const char *auth_token, const char *config, WT_FILE_SYSTEM **file_system)
{
    if (bucket == nullptr || strlen(bucket) == 0) {
        std::cerr << "azure_customize_file_system: Bucket not specified." << std::endl;
        return EINVAL;
    }

    // Get any prefix to be used for the object keys.
    WT_CONFIG_ITEM obj_prefix_config;
    std::string obj_prefix;

    // Get the value of the config key from the string
    azure_store *azure_storage = reinterpret_cast<azure_store *>(storage_source);
    int ret;
    if ((ret = azure_storage->wt_api->config_get_string(
           azure_storage->wt_api, session, config, "prefix", &obj_prefix_config)) == 0)
        obj_prefix = std::string(obj_prefix_config.str, obj_prefix_config.len);
    else if (ret != WT_NOTFOUND) {
        std::cerr << "azure_customize_file_system: error parsing config for object prefix."
                  << std::endl;
        return ret;
    }

    // Fetch the native WT file system.
    WT_FILE_SYSTEM *wt_file_system;
    if ((ret = azure_storage->wt_api->file_system_get(
           azure_storage->wt_api, session, &wt_file_system)) != 0)
        return ret;

    // Create file system and allocate memory for the file system.
    azure_file_system *azure_fs;
    try {
        azure_fs = new azure_file_system;
    } catch (std::bad_alloc &e) {
        std::cerr << std::string("azure_customize_file_system: ") + e.what() << std::endl;
        return ENOMEM;
    }

    // Initialise references to azure storage source, wt fs and home directory.
    azure_fs->store = azure_storage;
    azure_fs->wt_fs = wt_file_system;
    azure_fs->home_dir = session->connection->get_home(session->connection);
    try {
        azure_fs->azure_conn = std::make_unique<azure_connection>(bucket, obj_prefix);
    } catch (std::runtime_error &e) {
        std::cerr << std::string("azure_customize_file_system: ") + e.what() << std::endl;
        return ENOENT;
    }
    azure_fs->fs.fs_directory_list = azure_object_list;
    azure_fs->fs.fs_directory_list_single = azure_object_list_single;
    azure_fs->fs.fs_directory_list_free = azure_object_list_free;
    azure_fs->fs.terminate = azure_file_system_terminate;
    azure_fs->fs.fs_exist = azure_file_exists;
    azure_fs->fs.fs_open_file = azure_file_open;
    azure_fs->fs.fs_remove = azure_remove;
    azure_fs->fs.fs_rename = azure_rename;
    azure_fs->fs.fs_size = azure_object_size;

    // Add to the list of the active file systems. Lock will be freed when the scope is exited.
    {
        std::lock_guard<std::mutex> lock_guard(azure_storage->fs_mutex);
        azure_storage->azure_fs.push_back(azure_fs);
    }
    *file_system = &azure_fs->fs;
    return ret;
}

// Add a reference to the storage source so we can reference count to know when to terminate.
static int
azure_add_reference(WT_STORAGE_SOURCE *storage_source)
{
    azure_store *azure_storage = reinterpret_cast<azure_store *>(storage_source);
    if (azure_storage->reference_count == 0 || azure_storage->reference_count + 1 == 0) {
        std::cerr << "azure_add_reference: missing reference or overflow." << std::endl;
        return EINVAL;
    }
    ++azure_storage->reference_count;
    return 0;
}

static int
azure_flush(WT_STORAGE_SOURCE *storage_source, WT_SESSION *session, WT_FILE_SYSTEM *file_system,
  const char *source, const char *object, const char *config)
{
    WT_UNUSED(storage_source);
    WT_UNUSED(session);
    WT_UNUSED(file_system);
    WT_UNUSED(source);
    WT_UNUSED(object);
    WT_UNUSED(config);

    return 0;
}

static int
azure_flush_finish(WT_STORAGE_SOURCE *storage_source, WT_SESSION *session,
  WT_FILE_SYSTEM *file_system, const char *source, const char *object, const char *config)
{
    WT_UNUSED(storage_source);
    WT_UNUSED(session);
    WT_UNUSED(file_system);
    WT_UNUSED(source);
    WT_UNUSED(object);
    WT_UNUSED(config);

    return 0;
}

// Discard any resources on termination.
static int
azure_terminate(WT_STORAGE_SOURCE *storage_source, WT_SESSION *session)
{
    azure_store *azure_storage = reinterpret_cast<azure_store *>(storage_source);

    if (--azure_storage->reference_count != 0)
        return 0;

    /*
     * Terminate any active filesystems. There are no references to the storage source, so it is
     * safe to walk the active filesystem list without a lock. The removal from the list happens
     * under a lock. Also, removal happens from the front and addition at the end, so we are safe.
     */
    while (!azure_storage->azure_fs.empty()) {
        WT_FILE_SYSTEM *fs = reinterpret_cast<WT_FILE_SYSTEM *>(azure_storage->azure_fs.front());
        azure_file_system_terminate(fs, session);
    }

    delete azure_storage;
    return 0;
}

static int
azure_object_list(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *directory,
  const char *prefix, char ***dirlistp, uint32_t *countp)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(directory);
    WT_UNUSED(prefix);
    WT_UNUSED(dirlistp);
    WT_UNUSED(countp);

    return 0;
}

static int
azure_object_list_single(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *directory,
  const char *prefix, char ***dirlistp, uint32_t *countp)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(directory);
    WT_UNUSED(prefix);
    WT_UNUSED(dirlistp);
    WT_UNUSED(countp);

    return 0;
}

static int
azure_object_list_free(
  WT_FILE_SYSTEM *file_system, WT_SESSION *session, char **dirlist, uint32_t count)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(dirlist);

    return 0;
}

// Discard any resources on termination of the file system.
static int
azure_file_system_terminate(WT_FILE_SYSTEM *file_system, WT_SESSION *session)
{
    azure_file_system *azure_fs = reinterpret_cast<azure_file_system *>(file_system);
    azure_store *azure_storage = azure_fs->store;

    WT_UNUSED(session);

    // Remove from the active file system list. The lock will be freed when the scope is exited.
    {
        std::lock_guard<std::mutex> lock_guard(azure_storage->fs_mutex);
        // Erase-remove idiom used to eliminate specific file system.
        azure_storage->azure_fs.erase(
          std::remove(azure_storage->azure_fs.begin(), azure_storage->azure_fs.end(), azure_fs),
          azure_storage->azure_fs.end());
    }
    azure_fs->azure_conn.reset();
    free(azure_fs);
    return 0;
}

static int
azure_file_exists(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name, bool *existp)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(name);
    WT_UNUSED(existp);

    return 0;
}

static int
azure_remove(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name, uint32_t flags)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(name);
    WT_UNUSED(flags);

    return 0;
}

static int
azure_rename(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *from, const char *to,
  uint32_t flags)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(from);
    WT_UNUSED(to);
    WT_UNUSED(flags);

    return 0;
}

static int
azure_object_size(
  WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name, wt_off_t *sizep)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(name);
    WT_UNUSED(sizep);

    return 0;
}

static int
azure_file_open(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name,
  WT_FS_OPEN_FILE_TYPE file_type, uint32_t flags, WT_FILE_HANDLE **file_handlep)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(name);
    WT_UNUSED(file_type);
    WT_UNUSED(flags);
    WT_UNUSED(file_handlep);

    return 0;
}

static int
azure_file_close(WT_FILE_HANDLE *file_handle, WT_SESSION *session)
{
    WT_UNUSED(file_handle);
    WT_UNUSED(session);

    return 0;
}

static int
azure_file_lock(WT_FILE_HANDLE *file_handle, WT_SESSION *session, bool lock)
{
    WT_UNUSED(file_handle);
    WT_UNUSED(session);
    WT_UNUSED(lock);

    return 0;
}

static int
azure_file_read(
  WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t offset, size_t len, void *buf)
{
    WT_UNUSED(file_handle);
    WT_UNUSED(session);
    WT_UNUSED(offset);
    WT_UNUSED(len);
    WT_UNUSED(buf);

    return 0;
}

static int
azure_file_size(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t *sizep)
{
    WT_UNUSED(file_handle);
    WT_UNUSED(session);
    WT_UNUSED(sizep);

    return 0;
}

// An Azure storage source library - creates an entry point to the Azure extension.
int
wiredtiger_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config)
{
    azure_store *azure_storage = new azure_store;
    azure_storage->wt_api = connection->get_extension_api(connection);

    azure_storage->store.ss_customize_file_system = azure_customize_file_system;
    azure_storage->store.ss_add_reference = azure_add_reference;
    azure_storage->store.terminate = azure_terminate;
    azure_storage->store.ss_flush = azure_flush;
    azure_storage->store.ss_flush_finish = azure_flush_finish;

    // The first reference is implied by the call to add_storage_source.
    azure_storage->reference_count = 1;

    // Load the storage.
    if ((connection->add_storage_source(
          connection, "azure_store", &azure_storage->store, nullptr)) != 0) {
        std::cerr
          << "wiredtiger_extension_init: Could not load Azure storage source, shutting down."
          << std::endl;
        delete azure_storage;
        return -1;
    }
    return 0;
}
