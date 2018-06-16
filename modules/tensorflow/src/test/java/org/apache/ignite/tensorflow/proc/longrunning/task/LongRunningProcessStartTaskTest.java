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

package org.apache.ignite.tensorflow.proc.longrunning.task;

import org.apache.ignite.tensorflow.proc.longrunning.LongRunningProcess;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * Tests for {@link LongRunningProcessStartTask}.
 */
@RunWith(MockitoJUnitRunner.class)
public class LongRunningProcessStartTaskTest {
    /** Process metadata storage used instead of Apache Ignite node local storage. */
    private final Map<UUID, Future<?>> metadataStorage = new HashMap<>();

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
