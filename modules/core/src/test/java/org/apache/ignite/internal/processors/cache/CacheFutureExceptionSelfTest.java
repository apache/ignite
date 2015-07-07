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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import javax.cache.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * Cache future self test.
 */
public class CacheFutureExceptionSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = StartNode.createConfiguration();

        cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAsyncCacheFuture() throws Exception {
        final CountDownLatch readyLatch = new CountDownLatch(1);

        GridJavaProcess node1 = null;

        Collection<String> jvmArgs = Arrays.asList("-ea", "-DIGNITE_QUIET=false");

        try {
            node1 = GridJavaProcess.exec(
                StartNode.class.getName(), null,
                log,
                new CI1<String>() {
                    @Override public void apply(String s) {
                        info("Server node1: " + s);

                        if (s.contains("Topology snapshot"))
                            readyLatch.countDown();
                    }
                },
                null,
                jvmArgs,
                null
            );

            assertTrue(readyLatch.await(60, SECONDS));

            Ignite client = startGrid(0);

            IgniteCache<String, NotSerializableClass> cache = client.getOrCreateCache("CACHE");

            cache.put("key", new NotSerializableClass());

            System.setProperty("FAIL", "true");

            IgniteCache<String, NotSerializableClass> asyncCache = cache.withAsync();

            asyncCache.get("key");

            final CountDownLatch futLatch = new CountDownLatch(1);

            asyncCache.future().listen(new IgniteInClosure<IgniteFuture<Object>>() {
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

            assertTrue(futLatch.await(60, SECONDS));
        }
        finally {
            if (node1 != null)
                node1.killProcess();
        }
    }

    /**
     * Test class.
     */
    private static class NotSerializableClass implements Serializable {
        /** {@inheritDoc}*/
        private void writeObject(ObjectOutputStream out) throws IOException {
            out.writeObject(this);
        }

        /** {@inheritDoc}*/
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            if (System.getProperty("FAIL") != null)
                throw new RuntimeException("Deserialization failed.");

            in.readObject();
        }
    }

    /**
     * Test class.
     */
    public static class StartNode {
        /**
         * @return Configuration.
         */
        public static IgniteConfiguration createConfiguration() {
            IgniteConfiguration cfg = new IgniteConfiguration();

            cfg.setPeerClassLoadingEnabled(true);

            cfg.setLocalHost("127.0.0.1");

            TcpDiscoverySpi disco = new TcpDiscoverySpi();

            disco.setIpFinderCleanFrequency(1000);

            TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

            ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));

            disco.setIpFinder(ipFinder);

            cfg.setDiscoverySpi(disco);

            return cfg;
        }

        /**
         * @param args Main parameters.
         */
        public static void main(String[] args) {
            Ignition.start(createConfiguration());
        }
    }
}