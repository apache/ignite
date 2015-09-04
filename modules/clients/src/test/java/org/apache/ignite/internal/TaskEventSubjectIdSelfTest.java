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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.compute.ComputeTaskTimeoutException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVTS_TASK_EXECUTION;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;
import static org.apache.ignite.events.EventType.EVT_TASK_REDUCED;
import static org.apache.ignite.events.EventType.EVT_TASK_STARTED;
import static org.apache.ignite.events.EventType.EVT_TASK_TIMEDOUT;

/**
 * Tests for security subject ID in task events.
 */
public class TaskEventSubjectIdSelfTest extends GridCommonAbstractTest {
    /** */
    private static final Collection<TaskEvent> evts = new ArrayList<>();

    /** */
    private static CountDownLatch latch;

    /** */
    private static UUID nodeId;

    /** */
    private static GridClient client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite g = startGrid();

        g.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                assert evt instanceof TaskEvent;

                evts.add((TaskEvent)evt);

                latch.countDown();

                return true;
            }
        }, EVTS_TASK_EXECUTION);

        nodeId = g.cluster().localNode().id();

        GridClientConfiguration cfg = new GridClientConfiguration();

        cfg.setServers(Collections.singleton("127.0.0.1:11211"));

        client = GridClientFactory.start(cfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        GridClientFactory.stop(client.id());

        stopGrid();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        evts.clear();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleTask() throws Exception {
        latch = new CountDownLatch(3);

        grid().compute().execute(new SimpleTask(), null);

        assert latch.await(1000, MILLISECONDS);

        assertEquals(3, evts.size());

        Iterator<TaskEvent> it = evts.iterator();

        assert it.hasNext();

        TaskEvent evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_STARTED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_REDUCED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_FINISHED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert !it.hasNext();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailedTask() throws Exception {
        latch = new CountDownLatch(2);

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid().compute().execute(new FailedTask(), null);

                    return null;
                }
            },
            IgniteCheckedException.class,
            null
        );

        assert latch.await(1000, MILLISECONDS);

        assertEquals(2, evts.size());

        Iterator<TaskEvent> it = evts.iterator();

        assert it.hasNext();

        TaskEvent evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_STARTED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_FAILED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert !it.hasNext();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTimedOutTask() throws Exception {
        latch = new CountDownLatch(2);

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid().compute().withTimeout(100).execute(new TimedOutTask(), null);

                    return null;
                }
            },
            ComputeTaskTimeoutException.class,
            null
        );

        assert latch.await(1000, MILLISECONDS);

        assertEquals(3, evts.size());

        Iterator<TaskEvent> it = evts.iterator();

        assert it.hasNext();

        TaskEvent evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_STARTED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_TIMEDOUT, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_FAILED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert !it.hasNext();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosure() throws Exception {
        latch = new CountDownLatch(3);

        grid().compute().run(new IgniteRunnable() {
            @Override public void run() {
                // No-op.
            }
        });

        assert latch.await(1000, MILLISECONDS);

        assertEquals(3, evts.size());

        Iterator<TaskEvent> it = evts.iterator();

        assert it.hasNext();

        TaskEvent evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_STARTED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_REDUCED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_FINISHED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert !it.hasNext();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClient() throws Exception {
        latch = new CountDownLatch(3);

        client.compute().execute(SimpleTask.class.getName(), null);

        assert latch.await(1000, MILLISECONDS);

        assertEquals(3, evts.size());

        Iterator<TaskEvent> it = evts.iterator();

        assert it.hasNext();

        TaskEvent evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_STARTED, evt.type());
        assertEquals(client.id(), evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_REDUCED, evt.type());
        assertEquals(client.id(), evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_FINISHED, evt.type());
        assertEquals(client.id(), evt.subjectId());

        assert !it.hasNext();
    }

    /** */
    private static class SimpleTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            return Collections.singleton(new ComputeJobAdapter() {
                @Nullable @Override public Object execute() {
                    return null;
                }
            });
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /** */
    private static class FailedTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            return Collections.singleton(new ComputeJobAdapter() {
                @Nullable @Override public Object execute() {
                    return null;
                }
            });
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List<ComputeJobResult> results) {
            throw new IgniteException("Task failed.");
        }
    }

    /** */
    private static class TimedOutTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            return Collections.singleton(new ComputeJobAdapter() {
                @Nullable @Override public Object execute() {
                    try {
                        Thread.sleep(10000);
                    }
                    catch (InterruptedException ignored) {
                        // No-op.
                    }

                    return null;
                }
            });
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List<ComputeJobResult> results) {
            return null;
        }
    }
}