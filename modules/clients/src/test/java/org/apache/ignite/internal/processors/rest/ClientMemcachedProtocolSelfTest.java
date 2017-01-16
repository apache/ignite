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

package org.apache.ignite.internal.processors.rest;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Date;
import java.util.Map;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedClientIF;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Assert;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for TCP binary protocol.
 */
public class ClientMemcachedProtocolSelfTest extends AbstractRestProcessorSelfTest {
    /** Grid count. */
    private static final int GRID_CNT = 1;

    /** Custom port. */
    private Integer customPort;

    /** Memcache client. */
    private MemcachedClientIF client;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /**
     * @return Memcache client.
     * @throws Exception If start failed.
     */
    private MemcachedClientIF startClient() throws Exception {
        int port = customPort != null ? customPort : IgniteConfiguration.DFLT_TCP_PORT;

        return new MemcachedClient(new BinaryConnectionFactory(),
            F.asList(new InetSocketAddress(LOC_HOST, port)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        client = startClient();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        client.shutdown();

        customPort = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        assert cfg.getConnectorConfiguration() != null;

        if (customPort != null)
            cfg.getConnectorConfiguration().setPort(customPort);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        jcache().put("getKey1", "getVal1");
        jcache().put("getKey2", "getVal2");

        Assert.assertEquals("getVal1", client.get("getKey1"));
        Assert.assertEquals("getVal2", client.get("getKey2"));
        Assert.assertNull(client.get("wrongKey"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetBulk() throws Exception {
        jcache().put("getKey1", "getVal1");
        jcache().put("getKey2", "getVal2");
        jcache().put("getKey3", "getVal3");

        Map<String, Object> map = client.getBulk("getKey1", "getKey2");

        info("Map: " + map);

        Assert.assertEquals(2, map.size());

        Assert.assertEquals("getVal1", map.get("getKey1"));
        Assert.assertEquals("getVal2", map.get("getKey2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSet() throws Exception {
        Assert.assertTrue(client.set("setKey", 0, "setVal").get());

        assertEquals("setVal", jcache().get("setKey"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetWithExpiration() throws Exception {
        Assert.assertTrue(client.set("setKey", 2000, "setVal").get());

        assertEquals("setVal", jcache().get("setKey"));

        Thread.sleep(2100);

        Assert.assertNull(jcache().get("setKey"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAdd() throws Exception {
        jcache().put("addKey1", "addVal1");

        Assert.assertFalse(client.add("addKey1", 0, "addVal1New").get());
        Assert.assertTrue(client.add("addKey2", 0, "addVal2").get());

        assertEquals("addVal1", jcache().get("addKey1"));
        assertEquals("addVal2", jcache().get("addKey2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddWithExpiration() throws Exception {
        Assert.assertTrue(client.add("addKey", 2000, "addVal").get());

        assertEquals("addVal", jcache().get("addKey"));

        Thread.sleep(2100);

        Assert.assertNull(jcache().get("addKey"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplace() throws Exception {
        Assert.assertFalse(client.replace("replaceKey", 0, "replaceVal").get());

        Assert.assertNull(jcache().get("replaceKey"));
        jcache().put("replaceKey", "replaceVal");

        Assert.assertTrue(client.replace("replaceKey", 0, "replaceValNew").get());

        assertEquals("replaceValNew", jcache().get("replaceKey"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplaceWithExpiration() throws Exception {
        jcache().put("replaceKey", "replaceVal");

        Assert.assertTrue(client.set("replaceKey", 2000, "replaceValNew").get());

        assertEquals("replaceValNew", jcache().get("replaceKey"));

        Thread.sleep(2100);

        Assert.assertNull(jcache().get("replaceKey"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDelete() throws Exception {
        Assert.assertFalse(client.delete("deleteKey").get());

        jcache().put("deleteKey", "deleteVal");

        Assert.assertTrue(client.delete("deleteKey").get());

        Assert.assertNull(jcache().get("deleteKey"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncrement() throws Exception {
        Assert.assertEquals(5, client.incr("incrKey", 3, 2));

        assertEquals(5, grid(0).atomicLong("incrKey", 0, true).get());

        Assert.assertEquals(15, client.incr("incrKey", 10, 0));

        assertEquals(15, grid(0).atomicLong("incrKey", 0, true).get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDecrement() throws Exception {
        Assert.assertEquals(5, client.decr("decrKey", 10, 15));

        assertEquals(5, grid(0).atomicLong("decrKey", 0, true).get());

        Assert.assertEquals(2, client.decr("decrKey", 3, 0));

        assertEquals(2, grid(0).atomicLong("decrKey", 0, true).get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFlush() throws Exception {
        jcache().put("flushKey1", "flushVal1");
        jcache().put("flushKey2", "flushVal2");

        Assert.assertTrue(client.flush().get());

        Assert.assertNull(jcache().get("flushKey1"));
        Assert.assertNull(jcache().get("flushKey2"));
        Assert.assertTrue(jcache().localSize() == 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStat() throws Exception {
        jcache().put("statKey1", "statVal1");
        assertEquals("statVal1", jcache().get("statKey1"));

        Map<SocketAddress, Map<String, String>> map = client.getStats();

        Assert.assertEquals(1, map.size());

        Map<String, String> stats = F.first(map.values());

        Assert.assertEquals(4, stats.size());
        Assert.assertTrue(Integer.valueOf(stats.get("writes")) >= 1);
        Assert.assertTrue(Integer.valueOf(stats.get("reads")) >= 1);

        jcache().put("statKey2", "statVal2");
        assertEquals("statVal2", jcache().get("statKey2"));

        map = client.getStats();

        Assert.assertEquals(1, map.size());

        stats = F.first(map.values());

        Assert.assertEquals(4, stats.size());
        Assert.assertTrue(Integer.valueOf(stats.get("writes")) >= 2);
        Assert.assertTrue(Integer.valueOf(stats.get("reads")) >= 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAppend() throws Exception {
        Assert.assertFalse(client.append(0, "appendKey", "_suffix").get());

        jcache().put("appendKey", "appendVal");

        Assert.assertTrue(client.append(0, "appendKey", "_suffix").get());

        Assert.assertEquals("appendVal_suffix", client.get("appendKey"));

        assertEquals("appendVal_suffix", jcache().get("appendKey"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrepend() throws Exception {
        Assert.assertFalse(client.append(0, "prependKey", "_suffix").get());

        jcache().put("prependKey", "prependVal");

        Assert.assertTrue(client.append(0, "prependKey", "_suffix").get());

        Assert.assertEquals("prependVal_suffix", client.get("prependKey"));

        assertEquals("prependVal_suffix", jcache().get("prependKey"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSpecialTypes() throws Exception {
        Assert.assertTrue(client.set("boolKey", 0, true).get());

        Assert.assertEquals(true, client.get("boolKey"));

        Assert.assertTrue(client.set("intKey", 0, 10).get());

        Assert.assertEquals(10, client.get("intKey"));

        Assert.assertTrue(client.set("longKey", 0, 100L).get());

        Assert.assertEquals(100L, client.get("longKey"));

        Date now = new Date();

        Assert.assertTrue(client.set("dateKey", 0, now).get());

        Assert.assertEquals(now, client.get("dateKey"));

        Assert.assertTrue(client.set("byteKey", 0, (byte) 1).get());

        Assert.assertEquals((byte) 1, client.get("byteKey"));

        Assert.assertTrue(client.set("floatKey", 0, 1.1).get());

        Assert.assertEquals(1.1, client.get("floatKey"));

        Assert.assertTrue(client.set("doubleKey", 0, 100.001d).get());

        Assert.assertEquals(100.001d, client.get("doubleKey"));

        byte[] arr = new byte[5];

        for (byte i = 0; i < arr.length; i++)
            arr[i] = i;

        Assert.assertTrue(client.set("arrKey", 0, arr).get());

        assertArrayEquals(arr, (byte[])client.get("arrKey"));

        Assert.assertTrue(client.set("shortKey", 0, (short) 1).get());

        Assert.assertEquals((short) 1, client.get("shortKey"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testComplexObject() throws Exception {
        Assert.assertTrue(client.set("objKey", 0, new ValueObject(10, "String")).get());

        Assert.assertEquals(new ValueObject(10, "String"), client.get("objKey"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomPort() throws Exception {
        customPort = 11212;

        Ignite g = startGrid();

        assert g != null;
        assert g.cluster().nodes().size() == gridCount() + 1;

        MemcachedClientIF c = startClient();

        Assert.assertTrue(c.set("key", 0, 1).get());

        Assert.assertEquals(1, c.get("key"));

        c.shutdown();

        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    public void testVersion() throws Exception {
        Map<SocketAddress, String> map = client.getVersions();

        Assert.assertEquals(1, map.size());

        String ver = F.first(map.values());

        Assert.assertFalse(F.isEmpty(ver));
    }

    /**
     * Complex object.
     */
    private static class ValueObject implements Serializable {
        /** */
        private int intVal;

        /** */
        private String strVal;

        /**
         * @param intVal Integer value.
         * @param strVal String value.
         */
        private ValueObject(int intVal, String strVal) {
            this.intVal = intVal;
            this.strVal = strVal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ValueObject obj = (ValueObject)o;

            return intVal == obj.intVal &&
                !(strVal != null ? !strVal.equals(obj.strVal) : obj.strVal != null);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = intVal;

            res = 31 * res + (strVal != null ? strVal.hashCode() : 0);

            return res;
        }
    }
}
