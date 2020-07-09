/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class TxDeadlockDetectionMessageMarshallingTest extends GridCommonAbstractTest {
    /** Topic. */
    private static final String TOPIC = "mytopic";

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMessageUnmarshallWithoutCacheContext() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(ccfg);

            Ignite client = startClientGrid(1);

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
