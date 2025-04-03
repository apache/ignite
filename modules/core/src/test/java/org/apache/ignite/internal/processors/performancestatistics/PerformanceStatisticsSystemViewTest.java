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

package org.apache.ignite.internal.processors.performancestatistics;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests performance start with system views.
 */
public class PerformanceStatisticsSystemViewTest extends AbstractPerformanceStatisticsTest {
    /** */
    private static final List<String> IGNORED_VIEWS = List.of(
        "baseline.node.attributes",
        "metrics",
        "caches",
        "sql.queries",
        "nodes");

    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger();


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());
        cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testSystemViewCaches() throws Exception {
        LogListener lsnr = LogListener.matches("Finished writing system views to performance statistics file:").build();
        listeningLog.registerListener(lsnr);

        try (IgniteEx igniteEx = startGrid(0)) {
            startCollectStatistics();

            Set<String> viewsExpected = new HashSet<>();
            igniteEx.context().systemView().forEach(view -> {
                if (view.size() > 0 && !IGNORED_VIEWS.contains(view.name()))
                    viewsExpected.add(view.name());
            });

            assertTrue("Performance statistics writer did not start.", waitForCondition(lsnr::check, TIMEOUT));

            Set<String> viewsActual = new HashSet<>();
            stopCollectStatisticsAndRead(new TestHandler() {
                @Override public void systemView(UUID id, String name, List<String> schema, List<Object> row) {
                    viewsActual.add(name);
                }
            });

            assertEquals(viewsExpected, viewsActual);
        }
    }
}
