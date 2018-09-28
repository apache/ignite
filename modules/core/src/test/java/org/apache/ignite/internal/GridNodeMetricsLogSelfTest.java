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

package org.apache.ignite.internal;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.ExecutorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Check logging local node metrics
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "Kernal")
public class GridNodeMetricsLogSelfTest extends GridCommonAbstractTest {
    /** Executor name for setExecutorConfiguration */
    private static final String CUSTOM_EXECUTOR_0 = "Custom executor 0";

    /** Executor name for setExecutorConfiguration */
    private static final String CUSTOM_EXECUTOR_1 = "Custom executor 1";

    /** */
    private GridStringLogger strLog = new GridStringLogger(false, this.log);

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMetricsLogFrequency(1000);

        cfg.setExecutorConfiguration(new ExecutorConfiguration(CUSTOM_EXECUTOR_0),
            new ExecutorConfiguration(CUSTOM_EXECUTOR_1));

        cfg.setGridLogger(strLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        strLog.reset();

        strLog.logLength(300_000);

        startGrids(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeMetricsLog() throws Exception {
        IgniteCache<Integer, String> cache1 = grid(0).createCache("TestCache1");
        IgniteCache<Integer, String> cache2 = grid(1).createCache("TestCache2");

        cache1.put(1, "one");
        cache2.put(2, "two");

        Thread.sleep(10_000);

        // Check that nodes are alive.
        assertEquals("one", cache1.get(1));
        assertEquals("two", cache2.get(2));

        String logOutput = strLog.toString();

        checkNodeMetricsFormat(logOutput);

        checkMemoryMetrics(logOutput);
    }

    /**
     * Check node metrics format.
     *
     * @param fullLog Logging output.
     */
    protected void checkNodeMetricsFormat(String fullLog) {
        String msg = "Metrics are missing in the log or have an unexpected format";

        // Don't check the format strictly, but check that all expected metrics are present.
        assertTrue(msg, fullLog.contains("Metrics for local node (to disable set 'metricsLogFrequency' to 0)"));
        assertTrue(msg, fullLog.matches("(?s).*Node \\[id=.*, name=.*, uptime=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*H/N/C \\[hosts=.*, nodes=.*, CPUs=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*CPU \\[cur=.*, avg=.*, GC=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*PageMemory \\[pages=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*Heap \\[used=.*, free=.*, comm=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*Off-heap \\[used=.*, free=.*, comm=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).* region \\[used=.*, free=.*, comm=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*Outbound messages queue \\[size=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*Public thread pool \\[active=.*, idle=.*, qSize=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*System thread pool \\[active=.*, idle=.*, qSize=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*" + CUSTOM_EXECUTOR_0 + " \\[active=.*, idle=.*, qSize=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*" + CUSTOM_EXECUTOR_1 + " \\[active=.*, idle=.*, qSize=.*].*"));
    }

    /**
     * Check memory metrics values.
     *
     * @param logOutput Logging output.
     */
    protected void checkMemoryMetrics(String logOutput) {
        boolean summaryFmtMatches = false;

        Set<String> regions = new HashSet<>();

        Matcher matcher = Pattern.compile("(?m).{2,}( {3}(?<name>.+) region|Off-heap) " +
                "\\[used=(?<used>[-.\\d]*).*, free=(?<free>[-.\\d]*).*, comm=(?<comm>[-.\\d]*).*]")
            .matcher(logOutput);

        while (matcher.find()) {
            String subj = logOutput.substring(matcher.start(), matcher.end());

            assertFalse("\"used\" cannot be empty: " + subj, F.isEmpty(matcher.group("used")));
            assertFalse("\"free\" cannot be empty: " + subj, F.isEmpty(matcher.group("free")));
            assertFalse("\"comm\" cannot be empty: " + subj, F.isEmpty(matcher.group("comm")));

            int used = Integer.parseInt(matcher.group("used"));
            int comm = Integer.parseInt(matcher.group("comm"));
            double free = Double.parseDouble(matcher.group("free"));

            assertTrue(used + " should be non negative: " + subj, used >= 0);
            assertTrue(comm + " is less then " + used + ": " + subj, comm >= used);
            assertTrue(free + " is not between 0 and 100: " + subj, 0 <= free && free <= 100);

            String regName = matcher.group("name");

            if (F.isEmpty(regName))
                summaryFmtMatches = true;
            else
                regions.add(regName.trim());
        }

        assertTrue("Off-heap metrics have unexpected format.", summaryFmtMatches);

        Set<String> expRegions = grid(0).context().cache().context().database().dataRegions().stream()
            .map(v -> v.config().getName().trim())
            .collect(Collectors.toSet());

        assertEquals("Off-heap per-region metrics have unexpected format.", expRegions, regions);
    }
}
