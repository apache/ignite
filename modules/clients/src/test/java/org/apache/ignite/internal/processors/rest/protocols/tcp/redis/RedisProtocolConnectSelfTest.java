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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.apache.ignite.internal.util.IgniteUtils.KB;

/**
 * Tests for Connection commands of Redis protocol.
 */
public class RedisProtocolConnectSelfTest extends RedisCommonAbstractTest {
    /** */
    @Test
    public void testPing() {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals("PONG", jedis.ping());
        }
    }

    /** */
    @Test
    public void testEcho() {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals("Hello, grid!", jedis.echo("Hello, grid!"));
        }
    }

    /** */
    @Test
    public void testSelect() {
        try (Jedis jedis = pool.getResource()) {
            // connected to cache with index 0
            jedis.set("k0", "v0");
            Assert.assertEquals("v0", jedis.get("k0"));

            // connect to cache with index 1
            jedis.select(1);
            jedis.set("k1", "v1");
            Assert.assertEquals("v1", jedis.get("k1"));
            Assert.assertNull(jedis.get("k0"));

            try (Jedis jedis2 = pool.getResource()) {
                // connected to cache with index 0
                Assert.assertEquals("v0", jedis2.get("k0"));
                Assert.assertNull(jedis2.get("k1"));
            }

            Assert.assertEquals("v1", jedis.get("k1"));
            Assert.assertNull(jedis.get("k0"));

            jedis.select(0);
            Assert.assertEquals("v0", jedis.get("k0"));
        }
    }

    /** */
    @Test
    public void testClient() {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertNull(jedis.clientGetname());

            Assert.assertEquals("OK", jedis.clientSetname("test-client"));
            Assert.assertEquals("test-client", jedis.clientGetname());

            // The name is connection-scoped.
            try (Jedis jedis2 = pool.getResource()) {
                Assert.assertNull(jedis2.clientGetname());
            }

            Assert.assertEquals("test-client", jedis.clientGetname());
        }
    }

    /** */
    @Test
    public void testClientUnknownSubcommand() {
        try (Jedis jedis = pool.getResource()) {
            GridTestUtils.assertThrows(log, () -> jedis.clientUnpause(), JedisDataException.class,
                "Unknown subcommand 'UNPAUSE' for 'client' command");

            // The connection is still usable.
            Assert.assertEquals("PONG", jedis.ping());
        }
    }

    /** */
    @Test
    public void testSetGetLongString() {
        try (Jedis jedis = pool.getResource()) {
            for (int len : new int[] {8, 16, 32}) {
                String key = "b" + len;
                String val = RandomStringUtils.randomAscii((int)(len * KB));

                jedis.set(key.getBytes(), val.getBytes());
                Assert.assertArrayEquals(val.getBytes(), jedis.get(key.getBytes()));

                key += "-str";

                jedis.set(key, val);
                Assert.assertEquals(val, jedis.get(key));
            }
        }
    }
}
