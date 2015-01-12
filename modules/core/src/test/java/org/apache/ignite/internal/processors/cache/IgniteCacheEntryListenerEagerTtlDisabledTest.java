/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache;

import org.gridgain.grid.cache.*;

/**
 * Tests expire events when {@link GridCacheConfiguration#isEagerTtl()} is disabled.
 */
public class IgniteCacheEntryListenerEagerTtlDisabledTest extends IgniteCacheEntryListenerTxTest {
    /** {@inheritDoc} */
    @Override protected boolean eagerTtl() {
        return false;
    }
}
