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

package org.apache.ignite.internal.processors.cache.transform;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.client.thin.TcpClientCache;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.binary.GridBinaryMarshaller.TRANSFORMED;

/**
 * Checks transformation works via public and private cache API.
 */
@RunWith(Parameterized.class)
public class CacheObjectTransformationCacheApiTest extends GridCommonAbstractTest {
    /** Region name. */
    private static final String REGION_NAME = "region";

    /** Cache name. */
    protected static final String CACHE_NAME = "data";

    /** Nodes count. */
    protected static final int NODES = 3;

    /** Atomicity mode. */
    @Parameterized.Parameter()
    public CacheAtomicityMode mode;

    /** Persistence enabled flag. */
    @Parameterized.Parameter(1)
    public boolean persistence;

    /** Generator. */
    @Parameterized.Parameter(2)
    public Gen gen;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "mode={0}, persistence={1}, gen={2}")
    public static Collection<?> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (CacheAtomicityMode mode : CacheAtomicityMode.values())
            for (boolean persistence : new boolean[] {true, false})
                for (Gen gen : Gen.values())
                    res.add(new Object[] {mode, persistence, gen});

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        Ignite ignite = startGrids(NODES);

        if (persistence)
            ignite.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration drCgf = new DataRegionConfiguration()
            .setName(REGION_NAME)
            .setMaxSize(1000L * 1024 * 1024)
            .setInitialSize(1000L * 1024 * 1024);

        if (persistence)
            drCgf.setPersistenceEnabled(true);

        cfg.setPluginProviders(
            new TestCacheObjectTransformerPluginProvider(new CacheObjectDuplicatorTransformer()));

        CacheConfiguration<?, ?> cCfg = new CacheConfiguration<>(CACHE_NAME).setAtomicityMode(mode);

        cfg.setCacheConfiguration(cCfg);

        return cfg;
    }

    /** */
    @Test
    public void testCachePut() {
        Ignite ignite = grid(0);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < 100; i++) {
            Object val = gen.apply(i);

            cache.put(i, val);
            cache.put(i, val); // Override check.

            assertEqualsArraysAware(cache.get(i), val);
        }
    }

    /** */
    @Test
    public void testClientCachePut() {
        Ignite ignite = grid(0);

        String host = ignite.configuration().getLocalHost();
        int port = ignite.configuration().getClientConnectorConfiguration().getPort();

        try (IgniteClient client = G.startClient(new ClientConfiguration().setAddresses(host + ":" + port))) {
            ClientCache<Object, Object> cache = client.cache(CACHE_NAME);

            for (int i = 0; i < 100; i++) {
                Object val = gen.apply(i);

                if (i % 2 == 0) {
                    cache.put(i, val);
                    cache.put(i, val); // Override check.
                }
                else {
                    Map<Object, T3<Object, GridCacheVersion, Long>> data = new HashMap<>();

                    GridCacheVersion otherVer = new GridCacheVersion(1, 1, 1, 0);

                    data.put(i, new T3<>(val, otherVer, 0L));

                    ((TcpClientCache)cache).putAllConflict(data);
                    ((TcpClientCache)cache).putAllConflict(data); // Override check.
                }

                assertEqualsArraysAware(cache.get(i), val);
            }
        }
    }

    /**
     * Transforms each object with a shift.
     */
    private static final class CacheObjectDuplicatorTransformer extends TestCacheObjectTransformerProcessorAdapter {
        /** {@inheritDoc} */
        @Override public ByteBuffer transform(ByteBuffer original) {
            ByteBuffer transformed = ByteBuffer.wrap(new byte[original.remaining() * 2 + 1]);

            transformed.put(TRANSFORMED);

            while (original.hasRemaining()) {
                byte b = original.get();

                transformed.put(b);
                transformed.put(b);
            }

            transformed.flip();

            return transformed;
        }

        /** {@inheritDoc} */
        @Override public ByteBuffer restore(ByteBuffer transformed) {
            ByteBuffer restored = ByteBuffer.wrap(new byte[transformed.remaining() / 2]);

            while (transformed.hasRemaining()) {
                byte b1 = transformed.get();
                byte b2 = transformed.get();

                assertEquals(b1, b2);

                restored.put(b1);
            }

            restored.flip();

            return restored;
        }
    }

    /** */
    private static final class TestData {
        /** */
        int i;

        /** */
        public TestData(int i) {
            this.i = i;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestData data = (TestData)o;

            return i == data.i;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return i;
        }
    }

    /** */
    private enum Gen {
        /** */
        INT((i) -> i),

        /** */
        ARR((i) -> new int[] {i}),

        /** */
        STR(String::valueOf),

        /** */
        OBJ(TestData::new),

        /** */
        ARR_OBJ((i) -> new TestData[] {new TestData(i)});

        /** */
        private final Function<Integer, Object> gen;

        /** */
        Gen(Function<Integer, Object> gen) {
            this.gen = gen;
        }

        /** */
        private Object apply(Integer i) {
            return gen.apply(i);
        }
    }
}
