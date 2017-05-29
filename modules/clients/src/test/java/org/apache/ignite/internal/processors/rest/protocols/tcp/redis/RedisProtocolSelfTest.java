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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Tests for Redis protocol.
 */
public class RedisProtocolSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Local host. */
    private static final String HOST = "127.0.0.1";

    /** Port. */
    private static final int PORT = 6379;

    /** Pool. */
    private static JedisPool pool;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(gridCount());

        JedisPoolConfig jedisPoolCfg = new JedisPoolConfig();

        jedisPoolCfg.setMaxWaitMillis(10000);
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
        stopAllGrids();

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

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setStatisticsEnabled(true);
        ccfg.setIndexedTypes(String.class, String.class);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @return Cache.
     */
    @Override protected <K, V> IgniteCache<K, V> jcache() {
        return grid(0).cache(DEFAULT_CACHE_NAME);
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

    /**
     * @throws Exception If failed.
     */
    public void testPing() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals("PONG", jedis.ping());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEcho() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals("Hello, grid!", jedis.echo("Hello, grid!"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            jcache().put("getKey1", "getVal1");

            Assert.assertEquals("getVal1", jedis.get("getKey1"));
            Assert.assertNull(jedis.get("wrongKey"));

            jcache().put("setDataTypeKey", new HashSet<String>(Arrays.asList("1", "2")));

            try {
                jedis.get("setDataTypeKey");

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("WRONGTYPE"));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSet() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            jcache().put("getSetKey1", "1");

            Assert.assertEquals("1", jedis.getSet("getSetKey1", "0"));
            Assert.assertNull(jedis.get("getSetNonExistingKey"));

            jcache().put("setDataTypeKey", new HashSet<String>(Arrays.asList("1", "2")));

            try {
                jedis.getSet("setDataTypeKey", "0");

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("WRONGTYPE"));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMGet() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            jcache().put("getKey1", "getVal1");
            jcache().put("getKey2", 0);

            List<String> res = jedis.mget("getKey1", "getKey2", "wrongKey");

            Assert.assertTrue(res.contains("getVal1"));
            Assert.assertTrue(res.contains("0"));

//            not supported.
//            fail("Incompatible! getAll() does not return null values!");
//            Assert.assertTrue(result.contains("nil"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSet() throws Exception {
        long EXPIRE_MS = 1000L;
        int EXPIRE_SEC = 1;

        try (Jedis jedis = pool.getResource()) {
            jedis.set("setKey1", "1");
            jedis.set("setKey2".getBytes(), "b0".getBytes());

            Assert.assertEquals("1", jcache().get("setKey1"));
            Assert.assertEquals("b0", jcache().get("setKey2"));

            // test options.
            jedis.set("setKey1", "2", "nx");
            jedis.set("setKey3", "3", "nx", "px", EXPIRE_MS);

            Assert.assertEquals("1", jcache().get("setKey1"));
            Assert.assertEquals("3", jcache().get("setKey3"));

            jedis.set("setKey1", "2", "xx", "ex", EXPIRE_SEC);
            jedis.set("setKey4", "4", "xx");

            Assert.assertEquals("2", jcache().get("setKey1"));
            Assert.assertNull(jcache().get("setKey4"));

            // wait for expiration.
            Thread.sleep((long)(EXPIRE_MS * 1.2));

            Assert.assertNull(jcache().get("setKey1"));
            Assert.assertNull(jcache().get("setKey3"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMSet() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            jedis.mset("setKey1", "1", "setKey2", "2");

            Assert.assertEquals("1", jcache().get("setKey1"));
            Assert.assertEquals("2", jcache().get("setKey2"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncrDecr() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals(1, (long)jedis.incr("newKeyIncr"));
            Assert.assertEquals(-1, (long)jedis.decr("newKeyDecr"));

            Assert.assertEquals("1", jedis.get("newKeyIncr"));
            Assert.assertEquals("-1", jedis.get("newKeyDecr"));

            Assert.assertEquals(1, (long)jedis.incr("incrKey1"));

            jedis.set("incrKey1", "10");

            Assert.assertEquals(11L, (long)jedis.incr("incrKey1"));

            jedis.set("decrKey1", "10");

            Assert.assertEquals(9L, (long)jedis.decr("decrKey1"));

            jedis.set("nonInt", "abc");

            try {
                jedis.incr("nonInt");

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("ERR"));
            }

            try {
                jedis.decr("nonInt");

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("ERR"));
            }

            jedis.set("outOfRangeIncr1", "9223372036854775808");
            try {
                jedis.incr("outOfRangeIncr1");

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("ERR"));
            }

            jedis.set("outOfRangeDecr1", "-9223372036854775809");
            try {
                jedis.decr("outOfRangeDecr1");

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("ERR"));
            }

            jedis.set("outOfRangeInc2", String.valueOf(Long.MAX_VALUE));
            try {
                jedis.incr("outOfRangeInc2");

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("ERR"));
            }

            jedis.set("outOfRangeDecr2", String.valueOf(Long.MIN_VALUE));
            try {
                jedis.decr("outOfRangeDecr2");

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("ERR"));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncrDecrBy() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals(2, (long)jedis.incrBy("newKeyIncrBy", 2));
            Assert.assertEquals(-2, (long)jedis.decrBy("newKeyDecrBy", 2));

            jedis.set("incrDecrKeyBy", "1");

            Assert.assertEquals(11L, (long)jedis.incrBy("incrDecrKeyBy", 10));

            Assert.assertEquals(9L, (long)jedis.decrBy("incrDecrKeyBy", 2));

            jedis.set("outOfRangeIncrBy", "1");
            try {
                jedis.incrBy("outOfRangeIncrBy", Long.MAX_VALUE);

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("ERR"));
            }

            jedis.set("outOfRangeDecrBy", "-1");
            try {
                jedis.decrBy("outOfRangeDecrBy", Long.MIN_VALUE);

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("ERR"));
            }

            jedis.set("outOfRangeIncBy2", String.valueOf(Long.MAX_VALUE));
            try {
                jedis.incrBy("outOfRangeIncBy2", Long.MAX_VALUE);

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("ERR"));
            }

            jedis.set("outOfRangeDecrBy2", String.valueOf(Long.MIN_VALUE));
            try {
                jedis.decrBy("outOfRangeDecrBy2", Long.MIN_VALUE);

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("ERR"));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAppend() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals(5, (long)jedis.append("appendKey1", "Hello"));
            Assert.assertEquals(12, (long)jedis.append("appendKey1", " World!"));

            jcache().put("setDataTypeKey", new HashSet<String>(Arrays.asList("1", "2")));

            try {
                jedis.append("setDataTypeKey", "");

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("WRONGTYPE"));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStrlen() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals(0, (long)jedis.strlen("strlenKeyNonExisting"));

            jcache().put("strlenKey", "abc");

            Assert.assertEquals(3, (long)jedis.strlen("strlenKey"));

            jcache().put("setDataTypeKey", new HashSet<String>(Arrays.asList("1", "2")));

            try {
                jedis.strlen("setDataTypeKey");

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("WRONGTYPE"));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetRange() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals(0, (long)jedis.setrange("setRangeKey1", 0, ""));

            jcache().put("setRangeKey2", "abc");

            Assert.assertEquals(3, (long)jedis.setrange("setRangeKey2", 0, ""));

            Assert.assertEquals(3, (long)jedis.setrange("setRangeKeyPadded", 2, "a"));

            try {
                jedis.setrange("setRangeKeyWrongOffset", -1, "a");

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("ERR"));
            }

            try {
                jedis.setrange("setRangeKeyWrongOffset2", 536870911, "a");

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("ERR"));
            }

            jcache().put("setRangeKey3", "Hello World");

            Assert.assertEquals(11, (long)jedis.setrange("setRangeKey3", 6, "Redis"));

            jcache().put("setDataTypeKey", new HashSet<>(Arrays.asList("1", "2")));

            try {
                jedis.setrange("setDataTypeKey", 0, "Redis");

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("WRONGTYPE"));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetRange() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals("", jedis.getrange("getRangeKeyNonExisting", 0, 0));

            jcache().put("getRangeKey", "This is a string");

            Assert.assertEquals("This", jedis.getrange("getRangeKey", 0, 3));
            Assert.assertEquals("ing", jedis.getrange("getRangeKey", -3, -1));
            Assert.assertEquals("This is a string", jedis.getrange("getRangeKey", 0, -1));
            Assert.assertEquals("string", jedis.getrange("getRangeKey", 10, 100));

            jcache().put("setDataTypeKey", new HashSet<String>(Arrays.asList("1", "2")));

            try {
                jedis.getrange("setDataTypeKey", 0, 1);

                assert false : "Exception has to be thrown!";
            }
            catch (JedisDataException e) {
                assertTrue(e.getMessage().startsWith("WRONGTYPE"));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDel() throws Exception {
        jcache().put("delKey1", "abc");
        jcache().put("delKey2", "abcd");
        try (Jedis jedis = pool.getResource()) {
            // Should return the number of actually deleted entries.
//            Assert.assertEquals(0, (long)jedis.del("nonExistingDelKey"));
            Assert.assertEquals(2, (long)jedis.del("delKey1", "delKey2"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testExists() throws Exception {
        jcache().put("existsKey1", "abc");
        jcache().put("existsKey2", "abcd");
        try (Jedis jedis = pool.getResource()) {
            Assert.assertFalse(jedis.exists("nonExistingDelKey"));
            Assert.assertEquals(2, (long)jedis.exists("existsKey1", "existsKey2"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDbSize() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals(0, (long)jedis.dbSize());

            jcache().putAll(new HashMap<Integer, Integer>() {
                {
                    for (int i = 0; i < 100; i++)
                        put(i, i);
                }
            });

            Assert.assertEquals(100, (long)jedis.dbSize());
        }
    }
}
