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

package org.apache.ignite.internal.processors.rest.protocols.tcp.redis;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Common for all Redis tests.
 */
public class RedisCommonAbstractTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** Local host. */
    protected static final String HOST = "127.0.0.1";

    /** Port. */
    protected static final int PORT = 6379;

    /** Pool. */
    protected static JedisPool pool;

    /** Default Redis cache name. */
    private static final String DFLT_CACHE_NAME = "redis-ignite-internal-cache-0";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(gridCount());

        JedisPoolConfig jedisPoolCfg = new JedisPoolConfig();

        jedisPoolCfg.setMaxWaitMillis(20000);
        jedisPoolCfg.setMaxIdle(100);
        jedisPoolCfg.setMinIdle(1);
        jedisPoolCfg.setNumTestsPerEvictionRun(10);
        jedisPoolCfg.setTestOnBorrow(true);
        jedisPoolCfg.setTestOnReturn(true);
        jedisPoolCfg.setTestWhileIdle(true);
        jedisPoolCfg.setTimeBetweenEvictionRunsMillis(30000);

        pool = new JedisPool(jedisPoolCfg, HOST, PORT, 10000);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        pool.destroy();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setLocalHost(HOST);

        assert cfg.getConnectorConfiguration() == null;

        ConnectorConfiguration redisCfg = new ConnectorConfiguration();

        redisCfg.setHost(HOST);
        redisCfg.setPort(PORT);

        cfg.setConnectorConfiguration(redisCfg);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setStatisticsEnabled(true);
        ccfg.setIndexedTypes(String.class, String.class);
        ccfg.setName(DFLT_CACHE_NAME);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @return Cache.
     */
    @Override protected <K, V> IgniteCache<K, V> jcache() {
        return grid(0).cache(DFLT_CACHE_NAME);
    }

    /** */
    protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        assert grid(0).cluster().nodes().size() == gridCount();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        jcache().clear();

        assertTrue(jcache().localSize() == 0);
    }
}
