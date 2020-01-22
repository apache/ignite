/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jmx;

/**
 * Information and management operations for the given database.
 * @h2.resource
 *
 * @author Eric Dong
 * @author Thomas Mueller
 */
public interface DatabaseInfoMBean {

    /**
     * Is the database open in exclusive mode?
     * @h2.resource
     *
     * @return true if the database is open in exclusive mode, false otherwise
     */
    boolean isExclusive();

    /**
     * Is the database read-only?
     * @h2.resource
     *
     * @return true if the database is read-only, false otherwise
     */
    boolean isReadOnly();

    /**
     * The database compatibility mode (REGULAR if no compatibility mode is
     * used).
     * @h2.resource
     *
     * @return the database mode
     */
    String getMode();

    /**
     * Is multi-threading enabled?
     * @h2.resource
     *
     * @return true if multi-threading is enabled, false otherwise
     */
    boolean isMultiThreaded();

    /**
     * Is MVCC (multi version concurrency) enabled?
     * @h2.resource
     *
     * @return true if MVCC is enabled, false otherwise
     */
    boolean isMvcc();

    /**
     * The transaction log mode (0 disabled, 1 without sync, 2 enabled).
     * @h2.resource
     *
     * @return the transaction log mode
     */
    int getLogMode();

    /**
     * Set the transaction log mode.
     *
     * @param value the new log mode
     */
    void setLogMode(int value);

    /**
     * The number of write operations since the database was created.
     * @h2.resource
     *
     * @return the total write count
     */
    long getFileWriteCountTotal();

    /**
     * The number of write operations since the database was opened.
     * @h2.resource
     *
     * @return the write count
     */
    long getFileWriteCount();

    /**
     * The file read count since the database was opened.
     * @h2.resource
     *
     * @return the read count
     */
    long getFileReadCount();

    /**
     * The database file size in KB.
     * @h2.resource
     *
     * @return the number of pages
     */
    long getFileSize();

    /**
     * The maximum cache size in KB.
     * @h2.resource
     *
     * @return the maximum size
     */
    int getCacheSizeMax();

    /**
     * Change the maximum size.
     *
     * @param kb the cache size in KB.
     */
    void setCacheSizeMax(int kb);

    /**
     * The current cache size in KB.
     * @h2.resource
     *
     * @return the current size
     */
    int getCacheSize();

    /**
     * The database version.
     * @h2.resource
     *
     * @return the version
     */
    String getVersion();

    /**
     * The trace level (0 disabled, 1 error, 2 info, 3 debug).
     * @h2.resource
     *
     * @return the level
     */
    int getTraceLevel();

    /**
     * Set the trace level.
     *
     * @param level the new value
     */
    void setTraceLevel(int level);

    /**
     * List the database settings.
     * @h2.resource
     *
     * @return the database settings
     */
    String listSettings();

    /**
     * List sessions, including the queries that are in
     * progress, and locked tables.
     * @h2.resource
     *
     * @return information about the sessions
     */
    String listSessions();

}
