// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.cache.eviction;

import org.jetbrains.annotations.*;

import javax.cache.*;

/**
 * Evictable cache entry passed into {@link org.gridgain.grid.cache.eviction.GridCacheEvictionPolicy}.
 *
 * @author @java.author
 * @version @java.version
 */
public interface EvictableEntry extends Cache.Entry {
    /**
     * Attaches metadata to the entry.
     *
     * @param meta Metadata to attach. Pass {@code null} to remove previous value.
     * @return Previous metadata value.
     */
    public <T> T attachMeta(@Nullable Object meta);

    /**
     * Replaces entry metadata.
     *
     * @param oldMeta Old metadata value, possibly {@code null}.
     * @param newMeta New metadata value, possibly {@code null}.
     * @return {@code True} if metadata value was replaced.
     */
    public boolean replaceMeta(@Nullable Object oldMeta, @Nullable Object newMeta);
}
