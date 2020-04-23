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

import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * Tests for Connection commands of Redis protocol.
 */
public class RedisProtocolConnectSelfTest extends RedisCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPing() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals("PONG", jedis.ping());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEcho() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals("Hello, grid!", jedis.echo("Hello, grid!"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSelect() throws Exception {
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
}
