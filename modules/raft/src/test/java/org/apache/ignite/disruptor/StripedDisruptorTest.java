/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.disruptor;

import java.util.ArrayList;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.raft.jraft.disruptor.GroupAware;
import org.apache.ignite.raft.jraft.disruptor.StripedDisruptor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for striped disruptor.
 */
public class StripedDisruptorTest extends IgniteAbstractTest {
    /**
     * Checks the correctness of disruptor batching in a handler.
     * This test creates only one stripe in order to the real Disruptor is shared between two groups.
     *
     * @throws Exception If fialed.
     */
    @Test
    public void testDisruptorBatch() throws Exception {
        StripedDisruptor<GroupAwareTestObj> disruptor = new StripedDisruptor<>("test-disruptor",
            16384,
            GroupAwareTestObj::new,
            1);

        GroupAwareTestObjHandler handler1 = new GroupAwareTestObjHandler();
        GroupAwareTestObjHandler handler2 = new GroupAwareTestObjHandler();

        RingBuffer<GroupAwareTestObj> taskQueue1 = disruptor.subscribe("grp1", handler1);
        RingBuffer<GroupAwareTestObj> taskQueue2 = disruptor.subscribe("grp2", handler2);

        assertSame(taskQueue1, taskQueue2);

        for (int i = 0; i < 1_000; i++) {
            int finalInt = i;

            taskQueue1.tryPublishEvent((event, sequence) -> {
                event.groupId = "grp1";
                event.num = finalInt;
            });

            taskQueue2.tryPublishEvent((event, sequence) -> {
                event.groupId = "grp2";
                event.num = finalInt;
            });

            if (i % 10 == 0) {
                assertTrue(IgniteTestUtils.waitForCondition(() -> handler1.applied == finalInt + 1, 10_000),
                    LoggerMessageHelper.format("Batch was not commited [applied={}, expected={}, buffered={}]",
                        handler1.applied, finalInt + 1, handler1.batch));
                assertTrue(IgniteTestUtils.waitForCondition(() -> handler2.applied == finalInt + 1, 10_000),
                    LoggerMessageHelper.format("Batch was not commited [applied={}, expected={}, buffered={}]",
                        handler2.applied, finalInt + 1, handler2.batch));
            }
        }

        disruptor.shutdown();
    }

    /**
     * The test checks that the Striped Disruptor work same as real one
     * in the circumstances when we have only one consumer group.
     *
     * @throws Exception If fialed.
     */
    @Test
    public void testDisruptorSimple() throws Exception {
        StripedDisruptor<GroupAwareTestObj> disruptor = new StripedDisruptor<>("test-disruptor",
            16384,
            () -> new GroupAwareTestObj(),
            5);

        GroupAwareTestObjHandler handler = new GroupAwareTestObjHandler();

        RingBuffer<GroupAwareTestObj> taskQueue = disruptor.subscribe("grp", handler);

        for (int i = 0; i < 1_000; i++) {
            int finalInt = i;

            taskQueue.publishEvent((event, sequence) -> {
                event.groupId = "grp";
                event.num = finalInt;
            });
        }

        assertTrue(IgniteTestUtils.waitForCondition(() -> handler.applied == 1_000, 10_000));

        disruptor.shutdown();
    }

    /** Group event handler. */
    private static class GroupAwareTestObjHandler implements EventHandler<GroupAwareTestObj> {
        /** This is a container for the batch events. */
        ArrayList<Integer> batch = new ArrayList<>();

        /** Counter of applied events. */
        int applied = 0;

        /** {@inheritDoc} */
        @Override public void onEvent(GroupAwareTestObj event, long sequence, boolean endOfBatch) {
            batch.add(event.num);

            if (endOfBatch) {
                applied += batch.size();

                batch.clear();
            }
        }
    }

    /**
     * Group aware object implementation to test the striped disruptor.
     */
    private static class GroupAwareTestObj implements GroupAware {
        /** Group id. */
        String groupId;

        /** Any integer number. */
        int num;

        /**
         * Get a group id.
         *
         * @return Group id.
         */
        @Override public String groupId() {
            return groupId;
        }
    }
}
