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

import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/**
 * Tests performance statistics multiple start.
 */
public class PerformanceStatisticsMultipleStartTest extends AbstractPerformanceStatisticsTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testStartCreateNewFile() throws Exception {
        IgniteEx srv = startGrid(0);

        int cnt = 5;

        for (int i = 1; i <= cnt; i++) {
            startCollectStatistics();

            srv.cache(DEFAULT_CACHE_NAME).get(0);

            AtomicInteger ops = new AtomicInteger();

            stopCollectStatisticsAndRead(new TestHandler() {
                @Override public void cacheOperation(UUID nodeId, OperationType type, int cacheId, long startTime,
                    long duration) {
                    ops.incrementAndGet();
                }
            });

            assertEquals(i, ops.get());

            List<File> files = statisticsFiles();

            assertEquals(i, performanceStatisticsFiles(files).size());
            assertEquals(i, systemViewStatisticsFiles(files).size());
        }
    }
}
