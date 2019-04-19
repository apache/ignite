/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest.protocols.tcp.redis;

import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.Jedis;

/**
 * Tests for Server commands of Redis protocol.
 */
@RunWith(JUnit4.class)
public class RedisProtocolServerSelfTest extends RedisCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
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

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFlushDb() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals(0, (long)jedis.dbSize());

            jcache().putAll(new HashMap<Integer, Integer>() {
                {
                    for (int i = 0; i < 100; i++)
                        put(i, i);
                }
            });

            Assert.assertEquals(100, (long)jedis.dbSize());

            jedis.select(1);

            jcache().putAll(new HashMap<Integer, Integer>() {
                {
                    for (int i = 0; i < 100; i++)
                        put(i, i);
                }
            });

            // flush database 1.
            jedis.flushDB();

            Assert.assertEquals(0, (long)jedis.dbSize());

            jedis.select(0);

            Assert.assertEquals(100, (long)jedis.dbSize());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFlushAll() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals(0, (long)jedis.dbSize());

            for (int i = 0; i < 100; i++)
                jedis.set(String.valueOf(i), String.valueOf(i));

            Assert.assertEquals(100, (long)jedis.dbSize());

            jedis.select(1);

            for (int i = 0; i < 100; i++)
                jedis.set(String.valueOf(i), String.valueOf(i));

            Assert.assertEquals(100, (long)jedis.dbSize());

            jedis.flushAll();

            Assert.assertEquals(0, (long)jedis.dbSize());

            jedis.select(0);

            Assert.assertEquals(0, (long)jedis.dbSize());
        }
    }
}
