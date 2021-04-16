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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Tests for String commands of Redis protocol.
 */
public class RedisProtocolStringSelfTest extends RedisCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
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
    @Test
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
    @Test
    public void testMGetDirectOrder() throws Exception {
        testMGetOrder(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMGetReverseOrder() throws Exception {
        testMGetOrder(false);
    }

    /**
     * Tests mget operation.
     *
     * @param directOrder {@code true} if the order of inserting to a cache should be the same as the order using by mget.
     */
    public void testMGetOrder(boolean directOrder) {
        int keysCnt = 33;

        List<String> keys = new ArrayList<>(keysCnt);
        List<String> values = new ArrayList<>(keysCnt);

        // Fill values.
        for (int i = 0; i < keysCnt; ++i) {
            keys.add("getKey" + i);

            values.add("getValue" + i);
        }

        try (Jedis jedis = pool.getResource()) {
            for (int i = 0; i < keysCnt; ++i)
                jcache().put(keys.get(i), values.get(i));

            if (!directOrder) {
                Collections.reverse(keys);

                Collections.reverse(values);
            }

            List<String> res = jedis.mget(keys.toArray(new String[keysCnt]));

            Assert.assertEquals("The response size is not expected.", keysCnt, res.size());

            for (int i = 0; i < keysCnt; ++i)
                Assert.assertEquals(values.get(i), res.get(i));
        }
    }


    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMGetDuplicates() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            jcache().put("key-A", "value-A");
            jcache().put("key-B", "value-B");

            List<String> res = jedis.mget("key-A", "key-B", "key-A");

            Assert.assertEquals("The size of returned array must be equal to 3.", 3, res.size());

            Assert.assertEquals("value-A", res.get(0));
            Assert.assertEquals("value-B", res.get(1));
            Assert.assertEquals("value-A", res.get(2));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
    public void testExpire() throws Exception {
        testExpire(new Expiration() {
            @Override public long expire(Jedis jedis, String key) {
                return jedis.expire("k1", 2);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExpireMs() throws Exception {
        testExpire(new Expiration() {
            @Override public long expire(Jedis jedis, String key) {
                return jedis.pexpire("k1", 2000);
            }
        });
    }

    private void testExpire(Expiration exp) throws Exception {
        try (Jedis jedis = pool.getResource()) {
            jedis.set("k1", "v1");

            Assert.assertTrue(jedis.exists("k1"));

            Assert.assertEquals(1L, exp.expire(jedis, "k1"));

            Assert.assertEquals("v1", jedis.get("k1"));

            Thread.sleep(2100);

            Assert.assertFalse(jedis.exists("k1"));

            Assert.assertEquals(0L, (long)jedis.expire("k1", 2));
        }
    }

    private interface Expiration {
        long expire(Jedis jedis, String key);
    }
}
