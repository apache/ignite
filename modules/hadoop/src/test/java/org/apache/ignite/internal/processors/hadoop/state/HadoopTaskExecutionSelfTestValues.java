/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.hadoop.state;

import org.apache.ignite.internal.processors.hadoop.HadoopSharedMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared state for HadoopTaskExecutionSelfTest.
 */
public class HadoopTaskExecutionSelfTestValues {
    /** */
    private static HadoopSharedMap m = HadoopSharedMap.map(HadoopTaskExecutionSelfTestValues.class);

    /** Line count. */
    public static final AtomicInteger totalLineCnt = m.put("totalLineCnt", new AtomicInteger());

    /** Executed tasks. */
    public static final AtomicInteger executedTasks = m.put("executedTasks", new AtomicInteger());

    /** Cancelled tasks. */
    public static final AtomicInteger cancelledTasks = m.put("cancelledTasks", new AtomicInteger());

    /** Working directory of each task. */
    public static final Map<String, String> taskWorkDirs = m.put("taskWorkDirs",
        new ConcurrentHashMap<String, String>());

    /** Mapper id to fail. */
    public static final AtomicInteger failMapperId = m.put("failMapperId", new AtomicInteger());

    /** Number of splits of the current input. */
    public static final AtomicInteger splitsCount = m.put("splitsCount", new AtomicInteger());
}
