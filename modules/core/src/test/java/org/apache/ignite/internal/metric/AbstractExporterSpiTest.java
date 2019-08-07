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

package org.apache.ignite.internal.metric;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public abstract class AbstractExporterSpiTest extends GridCommonAbstractTest {
    /** */
    protected static final String FILTERED_PREFIX = "filtered.metric";

    /** */
    public static final int EXPORT_TIMEOUT = 1_000;

    /** */
    protected Set<String> EXPECTED_ATTRIBUTES = new HashSet<>(Arrays.asList(
        "TotalAllocatedSize",
        "LargeEntriesPagesCount",
        "PagesReplaced",
        "PhysicalMemorySize",
        "CheckpointBufferSize",
        "PagesReplaceRate",
        "AllocationRate",
        "PagesRead",
        "OffHeapSize",
        "UsedCheckpointBufferSize",
        "OffheapUsedSize",
        "EmptyDataPages",
        "PagesFillFactor",
        "DirtyPages",
        "EvictionRate",
        "PagesWritten",
        "TotalAllocatedPages",
        "PagesReplaceAge",
        "PhysicalMemoryPages"));

    /**
     * Creates some additional metrics.
     *
     * @param ignite Ignite.
     */
    protected void createAdditionalMetrics(IgniteEx ignite) {
        GridMetricManager mmgr = ignite.context().metric();

        mmgr.registry(FILTERED_PREFIX).longMetric("test", "").add(2);

        mmgr.registry("other.prefix").longMetric("test", "").add(42);

        mmgr.registry("other.prefix").longMetric("test2", "").add(43);

        mmgr.registry("other.prefix2").longMetric("test3", "").add(44);
    }
}
