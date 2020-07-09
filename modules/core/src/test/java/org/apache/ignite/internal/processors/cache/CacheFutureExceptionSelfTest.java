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

package org.apache.ignite.internal.processors.cache;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Cache future self test.
 */
public class CacheFutureExceptionSelfTest extends GridCommonAbstractTest {
    /** */
    private static volatile boolean fail;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(igniteInstanceName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAsyncCacheFuture() throws Exception {
        startGrid(0);

        startClientGrid(1);

        testGet(false, false);

        testGet(false, true);

        testGet(true, false);

        testGet(true, true);
    }

    /**
     * @param nearCache If {@code true} creates near cache on client.
     * @param cpyOnRead Cache copy on read flag.
     * @throws Exception If failed.
     */
    private void testGet(boolean nearCache, boolean cpyOnRead) throws Exception {
        if (MvccFeatureChecker.forcedMvcc()) {
            if (!MvccFeatureChecker.isSupported(MvccFeatureChecker.Feature.NEAR_CACHE))
                return;
        }

        fail = false;

        Ignite srv = grid(0);

        Ignite client = grid(1);

        final String cacheName = nearCache ? ("NEAR-CACHE-" + cpyOnRead) : ("CACHE-" + cpyOnRead);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setCopyOnRead(cpyOnRead);

        ccfg.setName(cacheName);

        IgniteCache<Object, Object> cache = srv.createCache(ccfg);

        cache.put("key", new NotSerializableClass());

        IgniteCache<Object, Object> clientCache = nearCache ? client.createNearCache(cacheName,
            new NearCacheConfiguration<>()) : client.cache(cacheName);

        fail = true;

        final CountDownLatch futLatch = new CountDownLatch(1);

        clientCache.getAsync("key").listen(new IgniteInClosure<IgniteFuture<Object>>() {
            @Override public void apply(IgniteFuture<Object> fut) {
                assertTrue(fut.isDone());

                try {
                    fut.get();

                    fail();
                }
                catch (CacheException e) {
                    log.info("Expected error: " + e);

                    futLatch.countDown();
                }
            }
        });

        assertTrue(futLatch.await(5, SECONDS));

        srv.destroyCache(cache.getName());
    }

    /**
     * Test class.
     */
    private static class NotSerializableClass implements Serializable {
        /** */
        private void writeObject(ObjectOutputStream out) throws IOException {
            out.writeObject(this);
        }

        /** */
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            if (fail)
                throw new RuntimeException("Deserialization failed.");

            in.readObject();
        }
    }
}
