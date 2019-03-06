/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.GridAtomicInteger;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.lang.IgniteFuture;
import org.junit.Test;

/**
 * Checks that number of concurrent asynchronous operations is limited when configuration parameter is set.
 */
public class GridCacheAsyncOperationsLimitSelfTest extends GridCacheAbstractSelfTest {
    /** */
    public static final int MAX_CONCURRENT_ASYNC_OPS = 50;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cCfg = super.cacheConfiguration(igniteInstanceName);

        cCfg.setMaxConcurrentAsyncOperations(MAX_CONCURRENT_ASYNC_OPS);

        return cCfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAsyncOps() throws Exception {
        final AtomicInteger cnt = new AtomicInteger();
        final GridAtomicInteger max = new GridAtomicInteger();

        for (int i = 0; i < 5000; i++) {
            final int i0 = i;

            cnt.incrementAndGet();

            jcache().putAsync("key" + i, i).listen(new CI1<IgniteFuture<?>>() {
                @Override public void apply(IgniteFuture<?> t) {
                    cnt.decrementAndGet();

                    max.setIfGreater(cnt.get());

                    if (i0 > 0 && i0 % 100 == 0)
                        info("cnt: " + cnt.get());
                }
            });

            assertTrue("Maximum number of permits exceeded: " + max.get(),  max.get() <= 51);
        }
    }
}
