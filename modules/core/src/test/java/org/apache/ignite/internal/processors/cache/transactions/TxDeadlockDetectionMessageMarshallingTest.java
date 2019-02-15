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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class TxDeadlockDetectionMessageMarshallingTest extends GridCommonAbstractTest {
    /** Topic. */
    private static final String TOPIC = "mytopic";

    /** Client mode. */
    private static boolean clientMode;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(clientMode);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMessageUnmarshallWithoutCacheContext() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(ccfg);

            clientMode = true;

            Ignite client = startGrid(1);

            final GridCacheSharedContext<Object, Object> clientCtx = ((IgniteKernal)client).context().cache().context();

            final CountDownLatch latch = new CountDownLatch(1);

            final AtomicBoolean res = new AtomicBoolean();

            clientCtx.gridIO().addMessageListener(TOPIC, new GridMessageListener() {
                @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                    if (msg instanceof TxLocksResponse) {
                        try {
                            ((TxLocksResponse)msg).finishUnmarshal(clientCtx, clientCtx.deploy().globalLoader());

                            res.set(true);
                        }
                        catch (Exception e) {
                            log.error("Message unmarshal failed", e);
                        }
                        finally {
                            latch.countDown();
                        }
                    }
                }
            });

            GridCacheContext cctx = ((IgniteCacheProxy)cache).context();

            KeyCacheObject key = cctx.toCacheKeyObject(1);

            TxLocksResponse msg = new TxLocksResponse();
            msg.addKey(cctx.txKey(key));

            msg.prepareMarshal(cctx.shared());

            ((IgniteKernal)ignite).context().cache().context().gridIO().sendToCustomTopic(
                ((IgniteKernal)client).localNode(), TOPIC, msg, GridIoPolicy.PUBLIC_POOL);

            boolean await = latch.await(1, TimeUnit.SECONDS);

            assertTrue(await && res.get());
        }
        finally {
            stopAllGrids();
        }
    }
}
