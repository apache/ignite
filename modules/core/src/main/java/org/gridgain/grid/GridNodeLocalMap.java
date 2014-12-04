/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.jetbrains.annotations.*;
import java.util.concurrent.*;

/**
 * Defines interface for node-local storage.
 * <p>
 * {@code GridNodeLocalMap} is similar to {@link ThreadLocal} in a way that its values are not
 * distributed and kept only on local node (similar like {@link ThreadLocal} values are attached to the
 * current thread only). Node-local values are used primarily by jobs executed from the remote
 * nodes to keep intermediate state on the local node between executions.
 * <p>
 * {@code GridNodeLocalMap} essentially is a {@link ConcurrentMap} with a few additional methods,
 * so it is fairly trivial to use.
 * <p>
 * You can get an instance of {@code GridNodeLocalMap} by calling {@link org.apache.ignite.IgniteCluster#nodeLocalMap()} method.
 */
public interface GridNodeLocalMap<K, V> extends ConcurrentMap<K, V>, GridMetadataAware {
    /**
     * Gets the value with given key. If that value does not exist, calls given closure
     * to get the default value, puts it into the map and returns it. If closure is {@code null}
     * return {@code null}.
     *
     * @param key Key to get the value for.
     * @param dflt Default value producing closure.
     * @return Value for the key or the value produced by the closure if key
     *      does not exist in the map. Return {@code null} if key is not found and
     *      closure is {@code null}.
     */
    public V addIfAbsent(K key, @Nullable Callable<V> dflt);

    /**
     * Unlike its sibling method {@link #putIfAbsent(Object, Object)} this method returns
     * current mapping from the map.
     *
     * @param key Key.
     * @param val Value to put if one does not exist.
     * @return Current mapping for a given key.
     */
    public V addIfAbsent(K key, V val);
}
