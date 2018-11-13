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

package org.apache.ignite.tensorflow.core.longrunning.task;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.ignite.tensorflow.core.longrunning.LongRunningProcess;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * Tests for {@link LongRunningProcessStartTask}.
 */
public class LongRunningProcessStartTaskTest {
    /** Process metadata storage used instead of Apache Ignite node local storage. */
    private final ConcurrentMap<UUID, Future<?>> metadataStorage = new ConcurrentHashMap<>();

    /** Initializes tests. */
    @Before
    public void init() {
        metadataStorage.clear();
    }

    /** */
    @Test
    public void testCall() throws ExecutionException, InterruptedException {
        LongRunningProcess proc = new LongRunningProcess(UUID.randomUUID(), () -> {});
        LongRunningProcessStartTask task = createTask(proc);
        List<UUID> procIds = task.call();

        assertEquals(1, procIds.size());

        UUID procId = procIds.get(0);

        assertNotNull(metadataStorage.get(procId));

        Future<?> fut = metadataStorage.get(procId);
        fut.get();

        assertEquals(true, fut.isDone());
    }

    /** */
    @Test(expected = ExecutionException.class)
    public void testCallWithException() throws ExecutionException, InterruptedException {
        LongRunningProcess proc = new LongRunningProcess(UUID.randomUUID(), () -> {
            throw new RuntimeException();
        });
        LongRunningProcessStartTask task = createTask(proc);
        List<UUID> procIds = task.call();

        assertEquals(1, procIds.size());

        UUID procId = procIds.get(0);

        assertNotNull(metadataStorage.get(procId));

        Future<?> fut = metadataStorage.get(procId);
        fut.get();
    }

    /**
     * Creates start task.
     *
     * @param proc Long running process.
     * @return Start task.
     */
    private LongRunningProcessStartTask createTask(LongRunningProcess proc) {
        LongRunningProcessStartTask startTask = new LongRunningProcessStartTask(Collections.singletonList(proc));

        startTask = spy(startTask);
        doReturn(metadataStorage).when(startTask).getMetadataStorage();

        return startTask;
    }
}
