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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.Jedis;

/**
 * Tests for Connection commands of Redis protocol.
 */
@RunWith(JUnit4.class)
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
