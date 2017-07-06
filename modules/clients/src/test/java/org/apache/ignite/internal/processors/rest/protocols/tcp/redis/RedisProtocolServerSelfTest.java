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

import java.util.HashMap;
import org.junit.Assert;
import redis.clients.jedis.Jedis;

/**
 * Tests for Server commands of Redis protocol.
 */
public class RedisProtocolServerSelfTest extends RedisCommonAbstractTest {
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

    /**
     * @throws Exception If failed.
     */
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

            jedis.flushDB();

            Assert.assertEquals(0, (long)jedis.dbSize());
        }
    }
}
