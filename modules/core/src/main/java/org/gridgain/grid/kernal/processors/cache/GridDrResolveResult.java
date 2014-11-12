/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

/**
 *
 */
public interface GridDrResolveResult<V> {
    /**
     * @return TTL.
     */
    public long newTtl();

    /**
     * @return Expire time.
     */
    public long newExpireTime();

    /**
     * @return DR expire time.
     */
    public long newDrExpireTime();

    /**
     * @return {@code True} in case merge is to be performed.
     */
    public boolean isMerge();

    /**
     * @return {@code True} in case old value should be used.
     */
    public boolean isUseOld();

    /**
     * @return Cache operation.
     */
    public GridCacheOperation operation();

    /**
     * @return Value.
     */
    public V value();

    /**
     * @return Value bytes.
     */
    public byte[] valueBytes();
}
