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

package org.apache.ignite.internal.performancestatistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter;
import org.apache.ignite.internal.processors.performancestatistics.OperationType;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.PERF_STAT_DIR;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_GET;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_PUT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_START;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CHECKPOINT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.JOB;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.PAGES_WRITE_THROTTLE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_READS;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TASK;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_COMMIT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_ROLLBACK;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests the performance statistics printer.
 */
public class PerformanceStatisticsPrinterTest {
    /** Test node ID. */
    private static final UUID NODE_ID = UUID.randomUUID();

    /** */
    @Before
    public void beforeTest() throws Exception {
        U.delete(new File(U.defaultWorkDirectory()));
    }

    /** */
    @After
    public void afterTest() throws Exception {
        U.delete(new File(U.defaultWorkDirectory()));
    }

    /** @throws Exception If failed. */
    @Test
    public void testOperationsFilter() throws Exception {
        createStatistics(writer -> {
            writer.cacheStart(0, "cache");
            writer.cacheOperation(CACHE_GET, 0, 0, 0);
            writer.transaction(GridIntList.asList(0), 0, 0, true);
            writer.transaction(GridIntList.asList(0), 0, 0, false);
            writer.query(GridCacheQueryType.SQL_FIELDS, "query", 0, 0, 0, true);
            writer.queryReads(GridCacheQueryType.SQL_FIELDS, NODE_ID, 0, 0, 0);
            writer.task(new IgniteUuid(NODE_ID, 0), "task", 0, 0, 0);
            writer.job(new IgniteUuid(NODE_ID, 0), 0, 0, 0, true);
            writer.checkpoint(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
            writer.pagesWriteThrottle(0, 0);
        });

        List<OperationType> expOps = F.asList(CACHE_START, CACHE_GET, TX_COMMIT, TX_ROLLBACK, QUERY, QUERY_READS,
            TASK, JOB, CHECKPOINT, PAGES_WRITE_THROTTLE);

        checkOperationFilter(null, expOps);
        checkOperationFilter(F.asList(CACHE_START), F.asList(CACHE_START));
        checkOperationFilter(F.asList(TASK, JOB), F.asList(TASK, JOB));
        checkOperationFilter(F.asList(CACHE_PUT), Collections.emptyList());
    }

    /** */
    private void checkOperationFilter(List<OperationType> opsArg, List<OperationType> expOps) throws Exception {
        List<String> args = new LinkedList<>();

        if (opsArg != null) {
            args.add("--ops");

            args.add(opsArg.stream().map(Enum::toString).collect(joining(",")));
        }

        List<OperationType> ops = new LinkedList<>(expOps);

        readStatistics(args, json -> {
            OperationType op = OperationType.valueOf(json.get("op").asText());

            assertTrue("Unexpected operation: " + op, ops.remove(op));

            UUID nodeId = UUID.fromString(json.get("nodeId").asText());

            assertEquals(NODE_ID, nodeId);
        });

        assertTrue("Expected operations:" + ops, ops.isEmpty());
    }

    /** @throws Exception If failed. */
    @Test
    public void testStartTimeFilter() throws Exception {
        long startTime1 = 10;
        long startTime2 = 20;

        createStatistics(writer -> {
            for (long startTime : new long[] {startTime1, startTime2}) {
                writer.cacheOperation(CACHE_GET, 0, startTime, 0);
                writer.transaction(GridIntList.asList(0), startTime, 0, true);
                writer.transaction(GridIntList.asList(0), startTime, 0, false);
                writer.query(GridCacheQueryType.SQL_FIELDS, "query", 0, startTime, 0, true);
                writer.task(new IgniteUuid(NODE_ID, 0), "", startTime, 0, 0);
                writer.job(new IgniteUuid(NODE_ID, 0), 0, startTime, 0, true);
                writer.checkpoint(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, startTime, 0, 0, 0);
                writer.pagesWriteThrottle(startTime, 0);
            }
        });

        checkStartTimeFilter(null, null, F.asList(startTime1, startTime2));
        checkStartTimeFilter(null, startTime1, F.asList(startTime1));
        checkStartTimeFilter(startTime2, null, F.asList(startTime2));
        checkStartTimeFilter(startTime1, startTime2, F.asList(startTime1, startTime2));
        checkStartTimeFilter(startTime2 + 1, null, Collections.emptyList());
        checkStartTimeFilter(null, startTime1 - 1, Collections.emptyList());
    }

    /** */
    private void checkStartTimeFilter(Long fromArg, Long toArg, List<Long> expTimes) throws Exception {
        List<OperationType> opsWithStartTime = F.asList(CACHE_GET, TX_COMMIT, TX_ROLLBACK, QUERY, TASK, JOB, CHECKPOINT,
            PAGES_WRITE_THROTTLE);

        List<String> args = new LinkedList<>();

        if (fromArg != null) {
            args.add("--from");

            args.add(fromArg.toString());
        }

        if (toArg != null) {
            args.add("--to");

            args.add(toArg.toString());
        }

        Map<Long, List<OperationType>> opsByTime = new HashMap<>();

        for (Long time : expTimes)
            opsByTime.put(time, new LinkedList<>(opsWithStartTime));

        readStatistics(args, json -> {
            OperationType op = OperationType.valueOf(json.get("op").asText());

            if (opsWithStartTime.contains(op)) {
                long startTime = json.get("startTime").asLong();

                assertTrue("Unexpected startTime: " + startTime, opsByTime.containsKey(startTime));
                assertTrue("Unexpected operation: " + op, opsByTime.get(startTime).remove(op));
            }
        });

        assertTrue("Expected operations: " + opsByTime, opsByTime.values().stream().allMatch(List::isEmpty));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCachesFilter() throws Exception {
        String cache1 = "cache1";
        String cache2 = "cache2";

        createStatistics(writer -> {
            for (String cache : new String[] {cache1, cache2}) {
                writer.cacheStart(CU.cacheId(cache), cache);
                writer.cacheOperation(CACHE_GET, CU.cacheId(cache), 0, 0);
                writer.transaction(GridIntList.asList(CU.cacheId(cache)), 0, 0, true);
                writer.transaction(GridIntList.asList(CU.cacheId(cache)), 0, 0, false);
            }
        });

        checkCachesFilter(null, new String[] {cache1, cache2});
        checkCachesFilter(new String[] {cache1}, new String[] {cache1});
        checkCachesFilter(new String[] {cache2}, new String[] {cache2});
        checkCachesFilter(new String[] {cache1, cache2}, new String[] {cache1, cache2});
        checkCachesFilter(new String[] {"unknown_cache"}, new String[0]);
    }

    /** */
    private void checkCachesFilter(String[] cachesArg, String[] expCaches) throws Exception {
        List<OperationType> cacheIdOps = F.asList(CACHE_START, CACHE_GET, TX_COMMIT, TX_ROLLBACK);

        List<String> args = new LinkedList<>();

        if (cachesArg != null) {
            args.add("--caches");

            args.add(String.join(",", cachesArg));
        }

        Map<Integer, List<OperationType>> opsById = new HashMap<>();

        for (String cache : expCaches)
            opsById.put(CU.cacheId(cache), new LinkedList<>(cacheIdOps));

        readStatistics(args, json -> {
            OperationType op = OperationType.valueOf(json.get("op").asText());

            Integer id = null;

            if (OperationType.cacheOperation(op) || op == CACHE_START)
                id = json.get("cacheId").asInt();
            else if (OperationType.transactionOperation(op)) {
                assertTrue(json.get("cacheIds").isArray());
                assertEquals(1, json.get("cacheIds").size());

                id = json.get("cacheIds").get(0).asInt();
            }
            else
                fail("Unexpected operation: " + op);

            assertTrue("Unexpected cache id: " + id, opsById.containsKey(id));
            assertTrue("Unexpected operation: " + op, opsById.get(id).remove(op));
        });

        assertTrue("Expected operations: " + opsById, opsById.values().stream().allMatch(List::isEmpty));
    }

    /** Writes statistics through passed writer. */
    private void createStatistics(Consumer<FilePerformanceStatisticsWriter> c) throws Exception {
        FilePerformanceStatisticsWriter writer = new FilePerformanceStatisticsWriter(new TestKernalContext(NODE_ID));

        writer.start();

        waitForCondition(() -> U.field((Object)U.field(writer, "fileWriter"), "runner") != null, 30_000);

        c.accept(writer);

        writer.stop();
    }

    /**
     * @param args Additional program arguments.
     * @param c Consumer to handle operations.
     * @throws Exception If failed.
     */
    private void readStatistics(List<String> args, Consumer<JsonNode> c) throws Exception {
        File perfStatDir = new File(U.defaultWorkDirectory(), PERF_STAT_DIR);

        assertTrue(perfStatDir.exists());

        File out = new File(U.defaultWorkDirectory(), "report.txt");

        U.delete(out);

        List<String> pArgs = new LinkedList<>();

        pArgs.add(perfStatDir.getAbsolutePath());
        pArgs.add("--out");
        pArgs.add(out.getAbsolutePath());

        pArgs.addAll(args);

        PerformanceStatisticsPrinter.main(pArgs.toArray(new String[0]));

        assertTrue(out.exists());

        ObjectMapper mapper = new ObjectMapper();

        try (BufferedReader reader = new BufferedReader(new FileReader(out))) {
            String line;

            while ((line = reader.readLine()) != null) {
                JsonNode json = mapper.readTree(line);

                assertTrue(json.isObject());

                UUID nodeId = UUID.fromString(json.get("nodeId").asText());

                assertEquals(NODE_ID, nodeId);

                c.accept(json);
            }
        }
    }

    /** Test kernal context. */
    private static class TestKernalContext extends GridTestKernalContext {
        /** Node ID. */
        private final UUID nodeId;

        /** @param nodeId Node ID. */
        public TestKernalContext(UUID nodeId) {
            super(new JavaLogger());

            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @Override public UUID localNodeId() {
            return nodeId;
        }
    }
}
