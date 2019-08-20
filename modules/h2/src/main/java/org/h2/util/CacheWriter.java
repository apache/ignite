/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import org.h2.message.Trace;

/**
 * The cache writer is called by the cache to persist changed data that needs to
 * be removed from the cache.
 */
public interface CacheWriter {

    /**
     * Persist a record.
     *
     * @param entry the cache entry
     */
    void writeBack(CacheObject entry);

    /**
     * Flush the transaction log, so that entries can be removed from the cache.
     * This is only required if the cache is full and contains data that is not
     * yet written to the log. It is required to write the log entries to the
     * log first, because the log is 'write ahead'.
     */
    void flushLog();

    /**
     * Get the trace writer.
     *
     * @return the trace writer
     */
    Trace getTrace();

}
