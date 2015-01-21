/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache;

import org.gridgain.grid.cache.store.*;

/**
 *
 */
public class IgniteCacheAtomicPrimaryWriteOrderWithStoreInvokeTest extends
    IgniteCacheAtomicPrimaryWriteOrderInvokeTest {
    /** {@inheritDoc} */
    @Override protected GridCacheStore<?, ?> cacheStore() {
        return new IgniteCacheAbstractTest.TestStore();
    }
}
