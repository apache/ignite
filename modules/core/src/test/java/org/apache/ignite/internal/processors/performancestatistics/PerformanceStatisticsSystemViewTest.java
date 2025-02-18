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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/**
 * Tests performance start with system views.
 */
public class PerformanceStatisticsSystemViewTest extends AbstractPerformanceStatisticsTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration().setAtomicityMode(CacheAtomicityMode.ATOMIC));

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testSystemViewCaches() throws Exception {
        try (IgniteEx igniteEx = startGrid(0)) {
            IgniteCache<Integer, Integer> cache = igniteEx.getOrCreateCache("myCache");
            cache.put(0, 0);

            startCollectStatistics();

            Set<String> viewsExpected = new HashSet<>(List.of(
                "partitionStates",
                "configuration",
                "distributed.metastorage",
                "local.cache.groups.io",
                "dataRegionPageLists"));

            stopCollectStatisticsAndRead(new TestHandler() {
                @Override public void systemView(UUID id, String name, Map<String, String> data) {
                    viewsExpected.remove(name);
                }
            });

            assertTrue("System views " + viewsExpected + " were not found.", viewsExpected.isEmpty());
        }
    }
}
