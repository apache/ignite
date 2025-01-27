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

package org.apache.ignite.internal.processors.query.calcite.exec.task;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** Tests QueryTasksQueue data structure. */
public class QueryTasksQueueTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testQueryBlockingUnblocking() throws Exception {
        long waitTimeout = 10_000L;

        QueryTasksQueue queue = new QueryTasksQueue();
        UUID qryId1 = UUID.randomUUID();
        UUID qryId2 = UUID.randomUUID();
        QueryKey qryKey1 = new QueryKey(qryId1, 0);
        QueryKey qryKey2 = new QueryKey(qryId2, 0);
        QueryKey qryKey3 = new QueryKey(qryId1, 1);

        queue.addTask(new QueryAwareTask(qryKey1, () -> {}));
        queue.addTask(new QueryAwareTask(qryKey1, () -> {}));
        queue.addTask(new QueryAwareTask(qryKey2, () -> {}));
        queue.addTask(new QueryAwareTask(qryKey2, () -> {}));
        queue.addTask(new QueryAwareTask(qryKey1, () -> {}));
        queue.addTask(new QueryAwareTask(qryKey3, () -> {}));

        QueryAwareTask task = queue.pollTaskAndBlockQuery(waitTimeout, TimeUnit.MILLISECONDS);
        assertEquals(qryKey1, task.queryKey());

        task = queue.pollTaskAndBlockQuery(waitTimeout, TimeUnit.MILLISECONDS);
        assertEquals(qryKey2, task.queryKey());

        task = queue.pollTaskAndBlockQuery(waitTimeout, TimeUnit.MILLISECONDS);
        assertEquals(qryKey3, task.queryKey());

        // Test threads parking and unparking.
        QueryAwareTask[] res = new QueryAwareTask[1];

        Runnable pollAndStoreResult = () -> {
            try {
                res[0] = queue.pollTaskAndBlockQuery(waitTimeout, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        // Unparking on unblock query.
        Thread thread1 = new Thread(pollAndStoreResult);

        thread1.start();

        assertTrue(GridTestUtils.waitForCondition(() -> thread1.getState() == Thread.State.TIMED_WAITING, waitTimeout));

        queue.unblockQuery(qryKey2);

        thread1.join(waitTimeout);

        assertFalse(thread1.isAlive());

        assertEquals(qryKey2, res[0].queryKey());

        // Unparking on new task.
        queue.unblockQuery(qryKey3);

        Thread thread2 = new Thread(pollAndStoreResult);

        thread2.start();

        assertTrue(GridTestUtils.waitForCondition(() -> thread2.getState() == Thread.State.TIMED_WAITING, waitTimeout));

        queue.addTask(new QueryAwareTask(qryKey3, () -> {}));

        thread2.join(waitTimeout);

        assertFalse(thread2.isAlive());

        assertEquals(qryKey3, res[0].queryKey());

        // Get next task after unblocking all pending locks.
        queue.unblockQuery(qryKey1);
        queue.unblockQuery(qryKey2);
        queue.unblockQuery(qryKey3);

        task = queue.pollTaskAndBlockQuery(waitTimeout, TimeUnit.MILLISECONDS);
        assertEquals(qryKey1, task.queryKey());

        // Unparking on unblock query second time.
        Thread thread3 = new Thread(pollAndStoreResult);

        thread3.start();

        assertTrue(GridTestUtils.waitForCondition(() -> thread3.getState() == Thread.State.TIMED_WAITING, waitTimeout));

        queue.unblockQuery(qryKey1);

        thread3.join(waitTimeout);

        assertFalse(thread3.isAlive());

        assertEquals(qryKey1, res[0].queryKey());

        assertEquals(0, queue.size());
    }

    /** */
    @Test
    public void testToArray() {
        QueryTasksQueue queue = new QueryTasksQueue();

        QueryKey qryKey1 = new QueryKey(UUID.randomUUID(), 0);
        QueryKey qryKey2 = new QueryKey(UUID.randomUUID(), 1);
        QueryKey qryKey3 = new QueryKey(UUID.randomUUID(), 2);

        queue.addTask(new QueryAwareTask(qryKey1, () -> {}));
        queue.addTask(new QueryAwareTask(qryKey2, () -> {}));
        queue.addTask(new QueryAwareTask(qryKey3, () -> {}));
        queue.addTask(new QueryAwareTask(qryKey1, () -> {}));

        assertEquals(4, queue.size());

        QueryAwareTask[] tasks = queue.toArray(new QueryAwareTask[2]);

        assertEquals(4, queue.size());
        assertEquals(4, tasks.length);
        assertEquals(qryKey1, tasks[0].queryKey());
        assertEquals(qryKey2, tasks[1].queryKey());
        assertEquals(qryKey3, tasks[2].queryKey());
        assertEquals(qryKey1, tasks[3].queryKey());

        tasks = queue.toArray(new QueryAwareTask[] {
            null, null, null, null,
            new QueryAwareTask(qryKey1, () -> {}),
            new QueryAwareTask(qryKey2, () -> {})
        });

        assertEquals(4, queue.size());
        assertEquals(6, tasks.length);
        assertEquals(qryKey1, tasks[0].queryKey());
        assertEquals(qryKey2, tasks[1].queryKey());
        assertEquals(qryKey3, tasks[2].queryKey());
        assertEquals(qryKey1, tasks[3].queryKey());
        assertNull(tasks[4]);
        assertNull(tasks[5]);
    }

    /** */
    @Test
    public void testDrainTo() {
        QueryTasksQueue queue = new QueryTasksQueue();

        QueryKey qryKey1 = new QueryKey(UUID.randomUUID(), 0);
        QueryKey qryKey2 = new QueryKey(UUID.randomUUID(), 1);
        QueryKey qryKey3 = new QueryKey(UUID.randomUUID(), 2);

        queue.addTask(new QueryAwareTask(qryKey1, () -> {}));
        queue.addTask(new QueryAwareTask(qryKey2, () -> {}));
        queue.addTask(new QueryAwareTask(qryKey3, () -> {}));
        queue.addTask(new QueryAwareTask(qryKey1, () -> {}));
        queue.addTask(new QueryAwareTask(qryKey2, () -> {}));
        queue.addTask(new QueryAwareTask(qryKey3, () -> {}));

        List<QueryAwareTask> tasks = new ArrayList<>();
        assertEquals(2, queue.drainTo(tasks, 2));

        assertEquals(4, queue.size());
        assertEquals(2, tasks.size());
        assertEquals(qryKey1, tasks.get(0).queryKey());
        assertEquals(qryKey2, tasks.get(1).queryKey());

        assertEquals(1, queue.drainTo(tasks, 1));

        assertEquals(3, queue.size());
        assertEquals(3, tasks.size());
        assertEquals(qryKey3, tasks.get(2).queryKey());

        tasks.clear();

        assertEquals(3, queue.drainTo(tasks, Integer.MAX_VALUE));

        assertEquals(0, queue.size());
        assertEquals(3, tasks.size());
        assertEquals(qryKey1, tasks.get(0).queryKey());
        assertEquals(qryKey2, tasks.get(1).queryKey());
        assertEquals(qryKey3, tasks.get(2).queryKey());
    }

    /** */
    @Test
    public void testRemove() {
        QueryTasksQueue queue = new QueryTasksQueue();

        QueryKey qryKey1 = new QueryKey(UUID.randomUUID(), 0);
        QueryKey qryKey2 = new QueryKey(UUID.randomUUID(), 1);
        QueryAwareTask task1 = new QueryAwareTask(qryKey1, () -> {});
        QueryAwareTask task2 = new QueryAwareTask(qryKey1, () -> {});
        QueryAwareTask task3 = new QueryAwareTask(qryKey2, () -> {});
        QueryAwareTask task4 = new QueryAwareTask(qryKey2, () -> {});

        queue.addTask(task1);
        queue.addTask(task2);
        queue.addTask(task3);
        queue.addTask(task4);

        assertEquals(4, queue.size());

        queue.removeTask(task2);

        assertEquals(3, queue.size());

        assertEqualsArraysAware(new QueryAwareTask[] {task1, task3, task4}, queue.toArray(new QueryAwareTask[3]));

        queue.removeTask(task1);

        assertEquals(2, queue.size());

        assertEqualsArraysAware(new QueryAwareTask[] {task3, task4}, queue.toArray(new QueryAwareTask[2]));

        queue.removeTask(task4);

        assertEquals(1, queue.size());

        assertEqualsArraysAware(new QueryAwareTask[] {task3}, queue.toArray(new QueryAwareTask[1]));

        queue.removeTask(task3);

        assertEquals(0, queue.size());

        // Check add after remove of all tasks.
        queue.addTask(task1);
        queue.addTask(task2);
        assertEquals(2, queue.size());

        assertEqualsArraysAware(new QueryAwareTask[] {task1, task2}, queue.toArray(new QueryAwareTask[2]));
    }
}
