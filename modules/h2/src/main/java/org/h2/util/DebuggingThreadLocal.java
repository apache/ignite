/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Similar to ThreadLocal, except that it allows its data to be read from other
 * threads - useful for debugging info.
 *
 * @param <T> the type
 */
public class DebuggingThreadLocal<T> {

    private final ConcurrentHashMap<Long, T> map = new ConcurrentHashMap<>();

    public void set(T value) {
        map.put(Thread.currentThread().getId(), value);
    }

    /**
     * Remove the value for the current thread.
     */
    public void remove() {
        map.remove(Thread.currentThread().getId());
    }

    public T get() {
        return map.get(Thread.currentThread().getId());
    }

    /**
     * Get a snapshot of the data of all threads.
     *
     * @return a HashMap containing a mapping from thread-id to value
     */
    public HashMap<Long, T> getSnapshotOfAllThreads() {
        return new HashMap<>(map);
    }

}
