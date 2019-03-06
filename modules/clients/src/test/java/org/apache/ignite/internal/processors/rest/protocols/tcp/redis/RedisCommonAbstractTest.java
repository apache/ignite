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

    /** {@inheritDoc} */
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
