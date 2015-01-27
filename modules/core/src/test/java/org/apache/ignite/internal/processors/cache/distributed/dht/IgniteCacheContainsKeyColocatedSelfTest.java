/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.cache.*;
import org.apache.ignite.internal.processors.cache.*;

/**
 *
 */
public class IgniteCacheContainsKeyColocatedSelfTest extends IgniteCacheContainsKeyAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheDistributionMode distributionMode() {
        return CacheDistributionMode.PARTITIONED_ONLY;
    }
}
