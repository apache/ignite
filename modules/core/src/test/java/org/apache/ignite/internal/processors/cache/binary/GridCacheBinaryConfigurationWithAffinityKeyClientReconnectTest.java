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

package org.apache.ignite.internal.processors.cache.binary;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteClientReconnectAbstractTest;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class GridCacheBinaryConfigurationWithAffinityKeyClientReconnectTest extends IgniteClientReconnectAbstractTest {
    /** */
    private static final String PAYLOAD = RandomStringUtils.random(100);

    /** */
    @Parameterized.Parameter
    public boolean binaryConfig;

    /** */
    @Parameterized.Parameter(1)
    public Object key;

    /** */
    private IgniteEx cli;

    /** */
    @Parameterized.Parameters(name = "with binary config = {0}, key = {1}")
    public static Collection<Object[]> parameters() {
        return Stream.of(true, false).flatMap(withBinary -> Stream.of(
                new Object[] { withBinary, TestNotAnnotatedKey.of(1)},
                new Object[] { withBinary, TestAnnotatedKey.of(1)}
        )).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheKeyConfiguration(
                new CacheKeyConfiguration(TestNotAnnotatedKey.class.getName(), TestNotAnnotatedKey.AFFINITY_KEY_FIELD)
        );

        if (binaryConfig) {
            cfg.setBinaryConfiguration(
                    new BinaryConfiguration()
                            .setTypeConfigurations(Arrays.asList(
                                    new BinaryTypeConfiguration()
                                            .setTypeName(TestNotAnnotatedKey.class.getName()),
                                    new BinaryTypeConfiguration()
                                            .setTypeName(TestAnnotatedKey.class.getName())
                            ))
            );
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 0;
    }


    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();

        cli = startClientGrid("client");

        assertTrue(cli.cluster().localNode().isClient());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testReconnectClientAffinityKeyGet() throws Exception {
        assertTrue(cli.cluster().localNode().isClient());

        final Ignite srv = clientRouter(cli);

        final IgniteCache<Object, Object> clientCache = cli.getOrCreateCache(DEFAULT_CACHE_NAME);

        final IgniteCache<Object, Object> srvCache = srv.cache(DEFAULT_CACHE_NAME);

        assertNotNull(srvCache);

        clientCache.put(key, PAYLOAD);

        assertEquals(PAYLOAD, clientCache.get(key));

        assertEquals(PAYLOAD, srvCache.get(key));

        reconnectClientNode(cli, srv, new Runnable() {
            @Override public void run() {
                assertNotNull(srvCache.get(key));
            }
        });

        assertEquals(PAYLOAD, srvCache.get(key));

        assertEquals(PAYLOAD, clientCache.get(key));
    }

    /** */
    @Test
    public void testReconnectClientAffinityKeyPartition() throws Exception {
        final Ignite srv = clientRouter(cli);

        cli.getOrCreateCache(DEFAULT_CACHE_NAME);

        final IgniteCache<TestNotAnnotatedKey, Object> srvCache = srv.cache(DEFAULT_CACHE_NAME);

        assertNotNull(srvCache);

        int partition = partition(key, cli);

        assertEquals(partition, partition(key, srv));

        reconnectClientNode(cli, srv, new Runnable() {
            @Override public void run() {
                assertEquals(partition, partition(key, srv));
            }
        });

        assertEquals(partition, partition(key, srv));

        assertEquals(partition, partition(key, cli));
    }

    /** */
    private <K> int partition(K key, Ignite ign) {
        return ign.affinity(DEFAULT_CACHE_NAME).partition(key);
    }

    /** */
    static class TestNotAnnotatedKey {
        /** */
        private static final String AFFINITY_KEY_FIELD = "notAnnotatedAffinityKey";

        /** */
        private final int notAnnotatedAffinityKey;

        /** */
        private final int nonAffinityInfo;

        /** */
        public TestNotAnnotatedKey(int affinityKey, int nonAffinityInfo) {
            notAnnotatedAffinityKey = affinityKey;
            this.nonAffinityInfo = nonAffinityInfo;
        }

        /** */
        public static TestNotAnnotatedKey of(int affinityKey) {
            return new TestNotAnnotatedKey(affinityKey, affinityKey);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestNotAnnotatedKey{" +
                    "notAnnotatedAffinityKey=" + notAnnotatedAffinityKey +
                    ", nonAffinityInfo=" + nonAffinityInfo +
                    '}';
        }
    }

    /** */
    static class TestAnnotatedKey {
        /** */
        @AffinityKeyMapped
        private final int annotatedAffinityKey;

        /** */
        private final int nonAffinityInfo;

        /** */
        public TestAnnotatedKey(int affinityKey, int nonAffinityInfo) {
            annotatedAffinityKey = affinityKey;
            this.nonAffinityInfo = nonAffinityInfo;
        }

        /** */
        public static TestAnnotatedKey of(int affinityKey) {
            return new TestAnnotatedKey(affinityKey, affinityKey);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestAnnotatedKey{" +
                    "annotatedAffinityKey=" + annotatedAffinityKey +
                    ", nonAffinityInfo=" + nonAffinityInfo +
                    '}';
        }
    }
}
