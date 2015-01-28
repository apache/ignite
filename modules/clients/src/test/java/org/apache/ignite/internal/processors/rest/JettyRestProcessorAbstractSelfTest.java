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

import org.apache.ignite.internal.util.typedef.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.*;

import static org.apache.ignite.IgniteSystemProperties.*;

/**
 * Tests for Jetty REST protocol.
 */
@SuppressWarnings("unchecked")
abstract class JettyRestProcessorAbstractSelfTest extends AbstractRestProcessorSelfTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(GG_JETTY_PORT, Integer.toString(restPort()));

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(GG_JETTY_PORT);
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /**
     * @return Port to use for rest. Needs to be changed over time
     *      because Jetty has some delay before port unbind.
     */
    protected abstract int restPort();

    /**
     * @param params Command parameters.
     * @return Returned content.
     * @throws Exception If failed.
     */
    private String content(Map<String, String> params) throws Exception {
        String addr = "http://" + LOC_HOST + ":" + restPort() + "/gridgain?";

        for (Map.Entry<String, String> e : params.entrySet())
            addr += e.getKey() + '=' + e.getValue() + '&';

        URL url = new URL(addr);

        URLConnection conn = url.openConnection();

        String signature = signature();

        if (signature != null)
            conn.setRequestProperty("X-Signature", signature);

        InputStream in = conn.getInputStream();

        LineNumberReader rdr = new LineNumberReader(new InputStreamReader(in));

        StringBuilder buf = new StringBuilder(256);

        for (String line = rdr.readLine(); line != null; line = rdr.readLine())
            buf.append(line);

        return buf.toString();
    }

    /**
     * @param json JSON response.
     * @param ptrn Pattern to match.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private void jsonEquals(String json, String ptrn) {
        assertTrue("JSON mismatch [json=" + json + ", ptrn=" + ptrn + ']', Pattern.matches(ptrn, json));
    }

    /**
     * @param res Response.
     * @param success Success flag.
     * @return Regex pattern for JSON.
     */
    private String cachePattern(String res, boolean success) {
        return "\\{\\\"affinityNodeId\\\":\\\"\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12}\\\"\\," +
            "\\\"error\\\":\\\"\\\"\\," +
            "\\\"response\\\":\\\"" + res + "\\\"\\," +
            "\\\"sessionToken\\\":\\\"\\\"," +
            "\\\"successStatus\\\":" + (success ? 0 : 1) + "\\}";
    }

    /**
     * @param res Response.
     * @param success Success flag.
     * @return Regex pattern for JSON.
     */
    private String integerPattern(int res, boolean success) {
        return "\\{\\\"error\\\":\\\"\\\"\\," +
            "\\\"response\\\":" + res + "\\," +
            "\\\"sessionToken\\\":\\\"\\\"," +
            "\\\"successStatus\\\":" + (success ? 0 : 1) + "\\}";
    }

    /**
     * @param res Response.
     * @param success Success flag.
     * @return Regex pattern for JSON.
     */
    private String cacheBulkPattern(String res, boolean success) {
        return "\\{\\\"affinityNodeId\\\":\\\"\\\"\\," +
            "\\\"error\\\":\\\"\\\"\\," +
            "\\\"response\\\":" + res + "\\," +
            "\\\"sessionToken\\\":\\\"\\\"," +
            "\\\"successStatus\\\":" + (success ? 0 : 1) + "\\}";
    }

    /**
     * @param res Response.
     * @param success Success flag.
     * @return Regex pattern for JSON.
     */
    private String cachePattern(boolean res, boolean success) {
        return "\\{\\\"affinityNodeId\\\":\\\"\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12}\\\"\\," +
            "\\\"error\\\":\\\"\\\"\\," +
            "\\\"response\\\":" + res + "\\," +
            "\\\"sessionToken\\\":\\\"\\\"," +
            "\\\"successStatus\\\":" + (success ? 0 : 1) + "\\}";
    }

    /**
     * @param res Response.
     * @param success Success flag.
     * @return Regex pattern for JSON.
     */
    private String cacheBulkPattern(boolean res, boolean success) {
        return "\\{\\\"affinityNodeId\\\":\\\"\\\"\\," +
            "\\\"error\\\":\\\"\\\"\\," +
            "\\\"response\\\":" + res + "\\," +
            "\\\"sessionToken\\\":\\\"\\\"," +
            "\\\"successStatus\\\":" + (success ? 0 : 1) + "\\}";
    }

    /**
     * @param res Response.
     * @param success Success flag.
     * @return Regex pattern for JSON.
     */
    private String cacheMetricsPattern(String res, boolean success) {
        return "\\{\\\"affinityNodeId\\\":\\\"(\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12})?\\\"\\," +
            "\\\"error\\\":\\\"\\\"\\," +
            "\\\"response\\\":" + res + "\\," +
            "\\\"sessionToken\\\":\\\"\\\"," +
            "\\\"successStatus\\\":" + (success ? 0 : 1) + "\\}";
    }

    /**
     * @param res Response.
     * @param success Success flag.
     * @return Regex pattern for JSON.
     */
    private String pattern(String res, boolean success) {
        return "\\{\\\"error\\\":\\\"" + (!success ? ".+" : "") + "\\\"\\," +
            "\\\"response\\\":" + res + "\\," +
            "\\\"sessionToken\\\":\\\"\\\"," +
            "\\\"successStatus\\\":" + (success ? 0 : 1) + "\\}";
    }

    /**
     * @param res Response.
     * @param success Success flag.
     * @return Regex pattern for JSON.
     */
    private String stringPattern(String res, boolean success) {
        return "\\{\\\"error\\\":\\\"" + (!success ? ".+" : "") + "\\\"\\," +
            "\\\"response\\\":\\\"" + res + "\\\"\\," +
            "\\\"sessionToken\\\":\\\"\\\"," +
            "\\\"successStatus\\\":" + (success ? 0 : 1) + "\\}";
    }

    /**
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        assertTrue(cache().putx("getKey", "getVal"));

        String ret = content(F.asMap("cmd", "get", "key", "getKey"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Get command result: " + ret);

        jsonEquals(ret, cachePattern("getVal", true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAll() throws Exception {
        assertTrue(cache().putx("getKey1", "getVal1"));
        assertTrue(cache().putx("getKey2", "getVal2"));

        String ret = content(F.asMap("cmd", "getall", "k1", "getKey1", "k2", "getKey2"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Get all command result: " + ret);

        jsonEquals(ret, cacheBulkPattern("\\{\\\"getKey1\\\":\\\"getVal1\\\"\\,\\\"getKey2\\\":\\\"getVal2\\\"\\}",
            true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        String ret = content(F.asMap("cmd", "put", "key", "putKey", "val", "putVal"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Put command result: " + ret);

        assertEquals("putVal", cache().peek("putKey"));

        jsonEquals(ret, cachePattern(true, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutWithExpiration() throws Exception {
        String ret = content(F.asMap("cmd", "put", "key", "putKey", "val", "putVal", "exp", "2000"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        jsonEquals(ret, cachePattern(true, true));

        assertEquals("putVal", cache().get("putKey"));

        Thread.sleep(2100);

        assertNull(cache().get("putKey"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAdd() throws Exception {
        assertTrue(cache().putx("addKey1", "addVal1"));

        String ret = content(F.asMap("cmd", "add", "key", "addKey2", "val", "addVal2"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        jsonEquals(ret, cachePattern(true, true));

        assertEquals("addVal1", cache().peek("addKey1"));
        assertEquals("addVal2", cache().peek("addKey2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddWithExpiration() throws Exception {
        String ret = content(F.asMap("cmd", "add", "key", "addKey", "val", "addVal", "exp", "2000"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        jsonEquals(ret, cachePattern(true, true));

        assertEquals("addVal", cache().get("addKey"));

        Thread.sleep(2100);

        assertNull(cache().get("addKey"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAll() throws Exception {
        String ret = content(F.asMap("cmd", "putall", "k1", "putKey1", "k2", "putKey2",
            "v1", "putVal1", "v2", "putVal2"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Put all command result: " + ret);

        assertEquals("putVal1", cache().peek("putKey1"));
        assertEquals("putVal2", cache().peek("putKey2"));

        jsonEquals(ret, cacheBulkPattern(true, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemove() throws Exception {
        assertTrue(cache().putx("rmvKey", "rmvVal"));

        assertEquals("rmvVal", cache().peek("rmvKey"));

        String ret = content(F.asMap("cmd", "rmv", "key", "rmvKey"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Remove command result: " + ret);

        assertNull(cache().peek("rmvKey"));

        jsonEquals(ret, cachePattern(true, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAll() throws Exception {
        assertTrue(cache().putx("rmvKey1", "rmvVal1"));
        assertTrue(cache().putx("rmvKey2", "rmvVal2"));
        assertTrue(cache().putx("rmvKey3", "rmvVal3"));
        assertTrue(cache().putx("rmvKey4", "rmvVal4"));

        assertEquals("rmvVal1", cache().peek("rmvKey1"));
        assertEquals("rmvVal2", cache().peek("rmvKey2"));
        assertEquals("rmvVal3", cache().peek("rmvKey3"));
        assertEquals("rmvVal4", cache().peek("rmvKey4"));

        String ret = content(F.asMap("cmd", "rmvall", "k1", "rmvKey1", "k2", "rmvKey2"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Remove all command result: " + ret);

        assertNull(cache().peek("rmvKey1"));
        assertNull(cache().peek("rmvKey2"));
        assertEquals("rmvVal3", cache().peek("rmvKey3"));
        assertEquals("rmvVal4", cache().peek("rmvKey4"));

        jsonEquals(ret, cacheBulkPattern(true, true));

        ret = content(F.asMap("cmd", "rmvall"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Remove all command result: " + ret);

        assertNull(cache().peek("rmvKey1"));
        assertNull(cache().peek("rmvKey2"));
        assertNull(cache().peek("rmvKey3"));
        assertNull(cache().peek("rmvKey4"));
        assertTrue(cache().isEmpty());

        jsonEquals(ret, cacheBulkPattern(true, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCas() throws Exception {
        assertTrue(cache().putx("casKey", "casOldVal"));

        assertEquals("casOldVal", cache().peek("casKey"));

        String ret = content(F.asMap("cmd", "cas", "key", "casKey", "val2", "casOldVal", "val1", "casNewVal"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("CAS command result: " + ret);

        assertEquals("casNewVal", cache().peek("casKey"));

        jsonEquals(ret, cachePattern(true, true));

        cache().remove("casKey");
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplace() throws Exception {
        assertTrue(cache().putx("repKey", "repOldVal"));

        assertEquals("repOldVal", cache().peek("repKey"));

        String ret = content(F.asMap("cmd", "rep", "key", "repKey", "val", "repVal"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Replace command result: " + ret);

        assertEquals("repVal", cache().peek("repKey"));

        jsonEquals(ret, cachePattern(true, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplaceWithExpiration() throws Exception {
        assertTrue(cache().putx("replaceKey", "replaceVal"));

        assertEquals("replaceVal", cache().get("replaceKey"));

        String ret = content(F.asMap("cmd", "rep", "key", "replaceKey", "val", "replaceValNew", "exp", "2000"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        jsonEquals(ret, cachePattern(true, true));

        assertEquals("replaceValNew", cache().get("replaceKey"));

        // Use larger value to avoid false positives.
        Thread.sleep(2100);

        assertNull(cache().get("replaceKey"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAppend() throws Exception {
        assertTrue(cache().putx("appendKey", "appendVal"));

        String ret = content(F.asMap("cmd", "append", "key", "appendKey", "val", "_suffix"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        jsonEquals(ret, cachePattern(true, true));

        assertEquals("appendVal_suffix", cache().get("appendKey"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrepend() throws Exception {
        assertTrue(cache().putx("prependKey", "prependVal"));

        String ret = content(F.asMap("cmd", "prepend", "key", "prependKey", "val", "prefix_"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        jsonEquals(ret, cachePattern(true, true));

        assertEquals("prefix_prependVal", cache().get("prependKey"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncrement() throws Exception {
        String ret = content(F.asMap("cmd", "incr", "key", "incrKey", "init", "2", "delta", "3"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        jsonEquals(ret, integerPattern(5, true));

        assertEquals(5, grid(0).atomicLong("incrKey", 0, true).get());

        ret = content(F.asMap("cmd", "incr", "key", "incrKey", "delta", "10"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        jsonEquals(ret, integerPattern(15, true));

        assertEquals(15, grid(0).atomicLong("incrKey", 0, true).get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDecrement() throws Exception {
        String ret = content(F.asMap("cmd", "decr", "key", "decrKey", "init", "15", "delta", "10"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        jsonEquals(ret, integerPattern(5, true));

        assertEquals(5, grid(0).atomicLong("decrKey", 0, true).get());

        ret = content(F.asMap("cmd", "decr", "key", "decrKey", "delta", "3"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        jsonEquals(ret, integerPattern(2, true));

        assertEquals(2, grid(0).atomicLong("decrKey", 0, true).get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCar() throws Exception {
        assertTrue(cache().putx("casKey", "casOldVal"));

        assertEquals("casOldVal", cache().peek("casKey"));

        String ret = content(F.asMap("cmd", "cas", "key", "casKey", "val2", "casOldVal"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("CAR command result: " + ret);

        assertNull(cache().peek("casKey"));

        jsonEquals(ret, cachePattern(true, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsent() throws Exception {
        assertNull(cache().peek("casKey"));

        String ret = content(F.asMap("cmd", "cas", "key", "casKey", "val1", "casNewVal"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("PutIfAbsent command result: " + ret);

        assertEquals("casNewVal", cache().peek("casKey"));

        jsonEquals(ret, cachePattern(true, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCasRemove() throws Exception {
        assertTrue(cache().putx("casKey", "casVal"));

        assertEquals("casVal", cache().peek("casKey"));

        String ret = content(F.asMap("cmd", "cas", "key", "casKey"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("CAS Remove command result: " + ret);

        assertNull(cache().peek("casKey"));

        jsonEquals(ret, cachePattern(true, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMetrics() throws Exception {
        String ret = content(F.asMap("cmd", "cache"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Cache metrics command result: " + ret);

        jsonEquals(ret, cacheMetricsPattern("\\{.+\\}", true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTopology() throws Exception {
        String ret = content(F.asMap("cmd", "top", "attr", "false", "mtr", "false"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Topology command result: " + ret);

        jsonEquals(ret, pattern("\\[\\{.+\\}\\]", true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testNode() throws Exception {
        String ret = content(F.asMap("cmd", "node", "attr", "true", "mtr", "true", "id",
            grid(0).localNode().id().toString()));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Topology command result: " + ret);

        jsonEquals(ret, pattern("\\{.+\\}", true));

        ret = content(F.asMap("cmd", "node", "attr", "false", "mtr", "false", "ip", LOC_HOST));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Topology command result: " + ret);

        jsonEquals(ret, pattern("\\{.+\\}", true));

        ret = content(F.asMap("cmd", "node", "attr", "false", "mtr", "false", "ip", LOC_HOST, "id",
            UUID.randomUUID().toString()));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Topology command result: " + ret);

        jsonEquals(ret, pattern("null", true));
    }

    /**
     * Tests {@code exe} command.
     * <p>
     * Note that attempt to execute unknown task (UNKNOWN_TASK) will result in exception on server.
     *
     * @throws Exception If failed.
     */
    public void testExe() throws Exception {
        String ret = content(F.asMap("cmd", "exe"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Exe command result: " + ret);

        jsonEquals(ret, pattern("null", false));

        // Attempt to execute unknown task (UNKNOWN_TASK) will result in exception on server.
        ret = content(F.asMap("cmd", "exe", "name", "UNKNOWN_TASK"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Exe command result: " + ret);

        jsonEquals(ret, pattern("null", false));

        grid(0).compute().localDeployTask(TestTask1.class, TestTask1.class.getClassLoader());
        grid(0).compute().localDeployTask(TestTask2.class, TestTask2.class.getClassLoader());

        ret = content(F.asMap("cmd", "exe", "name", TestTask1.class.getName()));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Exe command result: " + ret);

        jsonEquals(ret, pattern("\\{.+\\}", true));

        ret = content(F.asMap("cmd", "exe", "name", TestTask2.class.getName()));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Exe command result: " + ret);

        jsonEquals(ret, pattern("\\{.+" + TestTask2.RES + ".+\\}", true));

        ret = content(F.asMap("cmd", "res"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        info("Exe command result: " + ret);

        jsonEquals(ret, pattern("null", false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testVersion() throws Exception {
        String ret = content(F.asMap("cmd", "version"));

        assertNotNull(ret);
        assertTrue(!ret.isEmpty());

        jsonEquals(ret, stringPattern(".+", true));
    }

    protected abstract String signature() throws Exception;
}
