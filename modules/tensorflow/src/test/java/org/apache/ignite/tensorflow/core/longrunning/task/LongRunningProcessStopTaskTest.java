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
import org.apache.ignite.tensorflow.core.longrunning.task.util.LongRunningProcessState;
import org.apache.ignite.tensorflow.core.longrunning.task.util.LongRunningProcessStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link LongRunningProcessStopTask}.
 */
public class LongRunningProcessStopTaskTest {
    /** Process metadata storage used instead of Apache Ignite node local storage. */
    private final ConcurrentMap<UUID, Future<?>> metadataStorage = new ConcurrentHashMap<>();

    /** Initializes tests. */
    @Before
    public void init() {
        metadataStorage.clear();
    }

    /** Tests execution of the task in case process is not found. */
    @Test
    public void testCallProcessNotFound() {
        LongRunningProcessStopTask stopTask = createTask(UUID.randomUUID(), true);

        List<LongRunningProcessStatus> statuses = stopTask.call();

        assertEquals(1, statuses.size());

        LongRunningProcessStatus status = statuses.get(0);
        assertEquals(LongRunningProcessState.NOT_FOUND, status.getState());
        assertNull(status.getException());

        assertEquals(0, metadataStorage.size());
    }

    /** Tests execution of the task in case process is running. */
    @Test
    public void testCallProcessIsRunning() {
        UUID procId = UUID.randomUUID();

        Future<?> fut = mock(Future.class);
        doReturn(false).when(fut).isDone();
        metadataStorage.put(procId, fut);

        LongRunningProcessStopTask stopTask = createTask(procId, true);

        List<LongRunningProcessStatus> statuses = stopTask.call();

        assertEquals(1, statuses.size());
        verify(fut).cancel(eq(true));

        LongRunningProcessStatus status = statuses.get(0);
        assertEquals(LongRunningProcessState.DONE, status.getState());
        assertNull(status.getException());

        assertEquals(0, metadataStorage.size());
    }

    /** Tests execution of the task in case process is done. */
    @Test
    public void testCallProcessIsDone() {
        UUID procId = UUID.randomUUID();

        Future<?> fut = mock(Future.class);
        doReturn(true).when(fut).isDone();
        metadataStorage.put(procId, fut);

        LongRunningProcessStopTask stopTask = createTask(procId, true);

        List<LongRunningProcessStatus> statuses = stopTask.call();

        assertEquals(1, statuses.size());
        verify(fut).cancel(eq(true));

        LongRunningProcessStatus status = statuses.get(0);
        assertEquals(LongRunningProcessState.DONE, status.getState());
        assertNull(status.getException());

        assertEquals(0, metadataStorage.size());
    }

    /** Tests execution of the task in case process is done with exception. */
    @Test
    public void testCallProcessIsDoneWithException() throws ExecutionException, InterruptedException {
        UUID procId = UUID.randomUUID();

        Future<?> fut = mock(Future.class);
        doReturn(true).when(fut).isDone();
        doThrow(RuntimeException.class).when(fut).get();
        metadataStorage.put(procId, fut);

        LongRunningProcessStopTask stopTask = createTask(procId, true);

        List<LongRunningProcessStatus> statuses = stopTask.call();

        assertEquals(1, statuses.size());
        verify(fut).cancel(eq(true));

        LongRunningProcessStatus status = statuses.get(0);
        assertEquals(LongRunningProcessState.DONE, status.getState());
        assertNotNull(status.getException());
        assertTrue(status.getException() instanceof RuntimeException);

        assertEquals(0, metadataStorage.size());
    }

    /**
     * Creates stop task.
     *
     * @param procId Process identifier.
     * @return Stop task.
     */
    private LongRunningProcessStopTask createTask(UUID procId, boolean clear) {
        LongRunningProcessStopTask clearTask = new LongRunningProcessStopTask(Collections.singletonList(procId), clear);

        clearTask = Mockito.spy(clearTask);
        doReturn(metadataStorage).when(clearTask).getMetadataStorage();

        return clearTask;
    }
}
