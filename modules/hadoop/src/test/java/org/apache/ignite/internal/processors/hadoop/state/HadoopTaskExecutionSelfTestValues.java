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
    public static final AtomicInteger TOTAL_LINE_CNT = m.put("totalLineCnt", new AtomicInteger());

    /** Executed tasks. */
    public static final AtomicInteger EXECUTED_TASKS = m.put("executedTasks", new AtomicInteger());

    /** Cancelled tasks. */
    public static final AtomicInteger CANCELLED_TASKS = m.put("cancelledTasks", new AtomicInteger());

    /** Working directory of each task. */
    public static final Map<String, String> TASK_WORK_DIRS = m.put("taskWorkDirs",
        new ConcurrentHashMap<String, String>());

    /** Mapper id to fail. */
    public static final AtomicInteger FAIL_MAPPER_ID = m.put("failMapperId", new AtomicInteger());

    /** Number of splits of the current input. */
    public static final AtomicInteger SPLITS_COUNT = m.put("splitsCount", new AtomicInteger());
}
