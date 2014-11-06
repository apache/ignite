// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.services;

/**
 * Simple map service.
 *
 * @author @java.author
 * @version @java.version
 */
public interface SimpleMapService<K, V> {
    /**
     * Puts key-value pair into map.
     *
     * @param key Key.
     * @param val Value.
     */
    void put(K key, V val);

    /**
     * Gets value based on key.
     *
     * @param key Key.
     * @return Value.
     */
    V get(K key);

    /**
     * Clears map.
     */
    void clear();

    /**
     * @return Map size.
     */
    int size();
}
