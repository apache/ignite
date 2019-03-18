/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

public enum FileLockMethod {
    /**
     * This locking method means no locking is used at all.
     */
    NO,

    /**
     * This locking method means the cooperative file locking protocol should be
     * used.
     */
    FILE,

    /**
     * This locking method means a socket is created on the given machine.
     */
    SOCKET,

    /**
     * This locking method means multiple writers are allowed, and they
     * synchronize themselves.
     */
    SERIALIZED,

    /**
     * Use the file system to lock the file; don't use a separate lock file.
     */
    FS
}
