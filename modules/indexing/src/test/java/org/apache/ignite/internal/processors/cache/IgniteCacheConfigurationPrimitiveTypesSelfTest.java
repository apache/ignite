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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
@SuppressWarnings("unchecked")
public class IgniteCacheConfigurationPrimitiveTypesSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimitiveTypes() throws Exception {
        Ignite ignite = startGrid(1);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>("c1");

        ccfg.setIndexedTypes(
            byte.class, byte.class,
            short.class, short.class,
            int.class, int.class,
            long.class, long.class,
            float.class, float.class,
            double.class, double.class,
            boolean.class, boolean.class);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(ccfg);

        byte b = 1;
        cache.put(b, b);

        short s = 2;
        cache.put(s, s);

        int i = 3;
        cache.put(i, i);

        long l = 4;
        cache.put(l, l);

        float f = 5;
        cache.put(f, f);

        double d = 6;
        cache.put(d, d);

        boolean bool = true;
        cache.put(bool, bool);

        assert cache.query(new ScanQuery<>()).getAll().size() == 7;

        assertEquals(cache.query(new SqlQuery<>(Byte.class, "1 = 1")).getAll().size(), 1);
        assertEquals(cache.query(new SqlQuery<>(Short.class, "1 = 1")).getAll().size(), 1);
        assertEquals(cache.query(new SqlQuery<>(Integer.class, "1 = 1")).getAll().size(), 1);
        assertEquals(cache.query(new SqlQuery<>(Long.class, "1 = 1")).getAll().size(), 1);
        assertEquals(cache.query(new SqlQuery<>(Float.class, "1 = 1")).getAll().size(), 1);
        assertEquals(cache.query(new SqlQuery<>(Double.class, "1 = 1")).getAll().size(), 1);
        assertEquals(cache.query(new SqlQuery<>(Boolean.class, "1 = 1")).getAll().size(), 1);
    }
}