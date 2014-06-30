/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.query;

import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Entry used for continuous query notifications.
 */
public interface GridCacheContinuousQueryEntry<K, V> extends Map.Entry<K, V>, Serializable {
    /**
     * Gets entry key.
     *
     * @return Entry key.
     */
    @Override public K getKey();

    /**
     * Gets entry new value. New value may be null, if entry is being removed.
     *
     * @return Entry new value.
     */
    @Override @Nullable public V getValue();

    /**
     * Gets entry old value. Old value may be null if entry is being inserted (not updated).
     *
     * @return Gets entry old value.
     */
    @Nullable public V getOldValue();
}
