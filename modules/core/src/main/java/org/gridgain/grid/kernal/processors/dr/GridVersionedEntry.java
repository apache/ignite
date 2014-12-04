/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dr;

import org.apache.ignite.marshaller.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 *
 */
public interface GridVersionedEntry<K, V> extends Map.Entry<K, V> {
    /**
     * Gets entry's key.
     *
     * @return Entry's key.
     */
    public K key();

    /**
     * Gets entry's value.
     *
     * @return Entry's value.
     */
    @Nullable public V value();

    /**
     * Gets entry's TTL.
     *
     * @return Entry's TTL.
     */
    public long ttl();

    /**
     * Gets entry's expire time.
     *
     * @return Entry's expire time.
     */
    public long expireTime();

    /**
     * @return Version.
     */
    public GridCacheVersion version();

    /**
     * Perform internal marshal of this entry before it will be serialized.
     *
     * @param marsh Marshaller.
     * @throws GridException If failed.
     */
    public void marshal(IgniteMarshaller marsh) throws GridException;

    /**
     * Perform internal unmarshal of this entry. It must be performed after entry is deserialized and before
     * its restored key/value are needed.
     *
     * @param marsh Marshaller.
     * @throws GridException If failed.
     */
    public void unmarshal(IgniteMarshaller marsh) throws GridException;
}
