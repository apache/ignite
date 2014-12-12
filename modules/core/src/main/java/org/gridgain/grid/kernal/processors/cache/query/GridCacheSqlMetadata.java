/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.apache.ignite.spi.indexing.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Metadata for GridGain cache.
 * <p>
 * Metadata describes objects stored in the cache and
 * can be used to gather information about what can
 * be queried using GridGain cache queries feature.
 */
public interface GridCacheSqlMetadata extends Externalizable {
    /**
     * Cache name.
     *
     * @return Cache name.
     */
    public String cacheName();

    /**
     * Gets the collection of types stored in cache.
     * <p>
     * By default, type name is equal to simple class name
     * of stored object, but it can depend on implementation
     * of {@link GridIndexingSpi}.
     *
     * @return Collection of available types.
     */
    public Collection<String> types();

    /**
     * Gets key class name for provided type.
     * <p>
     * Use {@link #types()} method to get available types.
     *
     * @param type Type name.
     * @return Key class name or {@code null} if type name is unknown.
     */
    @Nullable public String keyClass(String type);

    /**
     * Gets value class name for provided type.
     * <p>
     * Use {@link #types()} method to get available types.
     *
     * @param type Type name.
     * @return Value class name or {@code null} if type name is unknown.
     */
    @Nullable public String valueClass(String type);

    /**
     * Gets fields and their class names for provided type.
     *
     * @param type Type name.
     * @return Fields map or {@code null} if type name is unknown.
     */
    @Nullable public Map<String, String> fields(String type);

    /**
     * Gets descriptors of indexes created for provided type.
     * See {@link GridCacheSqlIndexMetadata} javadoc for more information.
     *
     * @param type Type name.
     * @return Index descriptors.
     * @see GridCacheSqlIndexMetadata
     */
    public Collection<GridCacheSqlIndexMetadata> indexes(String type);
}
