/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.jdk8.backport.ConcurrentHashMap8;

import java.util.concurrent.ConcurrentMap;

/**
 * For tests.
 */
public class GridHadoopSharedMap {
    /** */
    private static ConcurrentMap<String, Object> map = new ConcurrentHashMap8<>();

    /**
     * Puts object by key.
     *
     * @param key Key.
     * @param val Value.
     */
    public static <T> T put(String key, T val) {
        Object old = map.putIfAbsent(key, val);

        return old == null ? val : (T)old;
    }

    /**
     * Clears.
     */
    public static void clear() {
        map.clear();
    }
}
