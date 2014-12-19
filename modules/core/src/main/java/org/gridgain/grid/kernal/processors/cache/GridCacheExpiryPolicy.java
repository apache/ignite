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

import java.util.*;

/**
 *
 */
public interface GridCacheExpiryPolicy {
    /**
     * @return TTL.
     */
    public abstract long forCreate();

    /**
     * @return TTL.
     */
    public abstract long forUpdate();

    /**
     * @return TTL.
     */
    public abstract long forAccess();

    /**
     * @param key Entry key.
     * @param keyBytes Entry key bytes.
     * @param ver Entry version.
     * @param rdrs Entry readers.
     */
    public void onAccessUpdated(Object key,
        byte[] keyBytes,
        GridCacheVersion ver,
        @Nullable Collection<UUID> rdrs);

    /**
     * @return Entries with TTL updated on access.
     */
    @Nullable public Map<Object, IgniteBiTuple<byte[], GridCacheVersion>> entries();

    /**
     * @return Readers for updated entries.
     */
    @Nullable Map<UUID, Collection<IgniteBiTuple<byte[], GridCacheVersion>>> readers();
}
