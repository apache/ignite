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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.google.common.collect.Collections2.permutations;
import static com.google.common.collect.Lists.cartesianProduct;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.PERF_STAT_DIR;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.writeIgniteUuid;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.writeString;

/**
 * Tests strings caching.
 */
@RunWith(Parameterized.class)
public class ForwardReadTest extends AbstractPerformanceStatisticsTest {
    /** Read buffer size. */
    private static final int BUFFER_SIZE = 100;

    /** {@code True} If test with strings that can't be found during forward read. */
    @Parameterized.Parameter
    public boolean unknownStrs;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "unknownStrs={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {{false}, {true}});
    }

    /** @throws Exception If failed. */
    @Test
    public void testStringForwardRead() throws Exception {
        File dir = U.resolveWorkDirectory(U.defaultWorkDirectory(), PERF_STAT_DIR, false);

        Map<String, Integer> expTasks = createStatistics(dir);

        new FilePerformanceStatisticsReader(BUFFER_SIZE, new TestHandler() {
            @Override public void task(UUID nodeId, IgniteUuid sesId, String taskName, long startTime, long duration,
                int affPartId) {
                assertNotNull(taskName);

                assertTrue(expTasks.containsKey(taskName));

                expTasks.computeIfPresent(taskName, (name, cnt) -> --cnt);
            }
        }).read(singletonList(dir));

        assertTrue(expTasks.values().stream().allMatch(cnt -> cnt == 0));
    }

    /** Creates test performance statistics file. */
    private Map<String, Integer> createStatistics(File dir) throws Exception {
        Map<String, Integer> expTasks;

        File file = new File(dir, "node-" + randomUUID() + ".prf");

        try (FileIO fileIo = new RandomAccessFileIOFactory().create(file)) {
            ByteBuffer buf = ByteBuffer.allocate(10 * 1024).order(ByteOrder.nativeOrder());

            expTasks = writeData(buf);

            buf.flip();

            fileIo.write(buf);

            fileIo.force();
        }

        return expTasks;
    }

    /** Generates task permutations and writes to buffer. */
    private Map<String, Integer> writeData(ByteBuffer buf) {
        Map<String, Integer> expTasks = new HashMap<>();

        List<List<Object>> lists = cartesianProduct(F.asList("task1", "task2"), F.asList(false, true));

        int setIdx = 0;

        for (List<List<Object>> permute : permutations(lists)) {
            for (List<Object> t2 : permute) {
                String taskName = "dataSet-" + setIdx + "-" + t2.get(0);
                Boolean cached = (Boolean)t2.get(1);

                expTasks.compute(taskName, (name, cnt) -> cnt == null ? 1 : ++cnt);

                writeTask(buf, taskName, cached);
            }

            if (unknownStrs) {
                String unknownTask = "dataSet-" + setIdx + "-unknownTask";

                expTasks.put(unknownTask, 0);

                writeTask(buf, unknownTask, true);
            }

            setIdx++;
        }

        return expTasks;
    }

    /** Writes test task to buffer. */
    private static void writeTask(ByteBuffer buf, String taskName, boolean cached) {
        buf.put((byte)OperationType.TASK.ordinal());
        writeString(buf, taskName, cached);
        writeIgniteUuid(buf, IgniteUuid.randomUuid());
        buf.putLong(0);
        buf.putLong(0);
        buf.putInt(0);
    }
}
