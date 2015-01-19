/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import javax.cache.expiry.*;
import java.util.*;

/**
 * Wrapper for {@link ExpiryPolicy} used to track information about cache entries
 * whose time to live was modified after access.
 */
public interface IgniteCacheExpiryPolicy {
    /**
     * @return TTL.
     */
    public long forCreate();

    /**
     * @return TTL.
     */
    public long forUpdate();

    /**
     * @return TTL.
     */
    public long forAccess();

    /**
     * Callback for ttl update on entry access.
     *
     * @param key Entry key.
     * @param keyBytes Entry key bytes.
     * @param ver Entry version.
     * @param rdrs Entry readers.
     */
    public void ttlUpdated(Object key,
       byte[] keyBytes,
       GridCacheVersion ver,
       @Nullable Collection<UUID> rdrs);

    /**
     * Clears information about updated entries.
     */
    public void reset();

    /**
     * @return Entries with TTL updated on access.
     */
    @Nullable public Map<Object, IgniteBiTuple<byte[], GridCacheVersion>> entries();

    /**
     * @return Readers for updated entries.
     */
    @Nullable Map<UUID, Collection<IgniteBiTuple<byte[], GridCacheVersion>>> readers();
}
