/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache;

import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

/**
 * Cache interceptor convenience adapter. It provides no-op implementations for all
 * interceptor callbacks.
 */
public class GridCacheInterceptorAdapter<K, V> implements GridCacheInterceptor<K, V> {
    /** {@inheritDoc} */
    @Nullable @Override public V onGet(K key, V val) {
        return val;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V onBeforePut(K key, @Nullable V oldVal, V newVal) {
        return newVal;
    }

    /** {@inheritDoc} */
    @Override public void onAfterPut(K key, V val) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteBiTuple<Boolean, V> onBeforeRemove(K key, @Nullable V val) {
        return new IgniteBiTuple<>(false, val);
    }

    /** {@inheritDoc} */
    @Override public void onAfterRemove(K key, V val) {
        // No-op.
    }
}
