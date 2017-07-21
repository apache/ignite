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
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

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

        IgniteCache<Byte, Byte> cacheByte = jcache(ignite, new CacheConfiguration(DEFAULT_CACHE_NAME), byte.class, byte.class);
        byte b = 1;
        cacheByte.put(b, b);

        IgniteCache<Short, Short> cacheShort = jcache(ignite, new CacheConfiguration(DEFAULT_CACHE_NAME), short.class, short.class);
        short s = 2;
        cacheShort.put(s, s);

        IgniteCache<Integer, Integer> cacheInt = jcache(ignite, new CacheConfiguration(DEFAULT_CACHE_NAME), int.class, int.class);
        int i = 3;
        cacheInt.put(i, i);

        IgniteCache<Long, Long> cacheLong = jcache(ignite, new CacheConfiguration(DEFAULT_CACHE_NAME), long.class, long.class);
        long l = 4;
        cacheLong.put(l, l);

        IgniteCache<Float, Float> cacheFloat = jcache(ignite, new CacheConfiguration(DEFAULT_CACHE_NAME), float.class, float.class);
        float f = 5;
        cacheFloat.put(f, f);

        IgniteCache<Double, Double> cacheDouble = jcache(ignite, new CacheConfiguration(DEFAULT_CACHE_NAME), double.class, double.class);
        double d = 6;
        cacheDouble.put(d, d);

        IgniteCache<Boolean, Boolean> cacheBoolean = jcache(ignite, new CacheConfiguration(DEFAULT_CACHE_NAME), boolean.class, boolean.class);
        boolean bool = true;
        cacheBoolean.put(bool, bool);


        assertEquals(cacheByte.query(new SqlQuery<>(Byte.class, "1 = 1")).getAll().size(), 1);
        assertEquals(cacheShort.query(new SqlQuery<>(Short.class, "1 = 1")).getAll().size(), 1);
        assertEquals(cacheInt.query(new SqlQuery<>(Integer.class, "1 = 1")).getAll().size(), 1);
        assertEquals(cacheLong.query(new SqlQuery<>(Long.class, "1 = 1")).getAll().size(), 1);
        assertEquals(cacheFloat.query(new SqlQuery<>(Float.class, "1 = 1")).getAll().size(), 1);
        assertEquals(cacheDouble.query(new SqlQuery<>(Double.class, "1 = 1")).getAll().size(), 1);
        assertEquals(cacheBoolean.query(new SqlQuery<>(Boolean.class, "1 = 1")).getAll().size(), 1);
    }
}