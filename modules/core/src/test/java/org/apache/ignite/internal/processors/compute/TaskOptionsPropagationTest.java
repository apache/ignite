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

package org.apache.ignite.internal.processors.compute;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.ignite.internal.processors.compute.TaskOptionsPropagationTest.ComputationType.CALLABLE;
import static org.apache.ignite.internal.processors.compute.TaskOptionsPropagationTest.ComputationType.CLOSURE;
import static org.apache.ignite.internal.processors.compute.TaskOptionsPropagationTest.ComputationType.RUNNABLE;
import static org.apache.ignite.internal.processors.compute.TaskOptionsPropagationTest.ComputationType.TASK;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
public class TaskOptionsPropagationTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_TASK_NAME = "test-name";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();

        grid().createCache(DEFAULT_CACHE_NAME).put(0, 0);
    }

    /** */
    @Test
    public void testUserTaskOptionsWithPrecedingSystemTaskExecution() throws Exception {
        try (IgniteEx cli = startClientGrid(1)) {
            cli.compute().withName(TEST_TASK_NAME).affinityCall(
                DEFAULT_CACHE_NAME,
                0,
                new TestCallable(TEST_TASK_NAME)
            );
        }
    }

    /** */
    @Test
    public void testComputeSharedAcrossMultipleThreads() throws Exception {
        IgniteCompute compute = grid().compute();

        compute.withName(TEST_TASK_NAME);

        runAsync(() -> compute.call(new TestCallable(TestCallable.class.getName()))).get();

        compute.call(new TestCallable(TEST_TASK_NAME));
    }

    /** */
    @Test
    public void testTaskExecutionOptionsReset() throws Exception {
        check(TASK, (c, t) -> c.execute((ComputeTask)t, null));
        check(TASK, (c, t) -> c.executeAsync((ComputeTask)t, null).get());

        check(CALLABLE, (c, t) -> c.call((IgniteCallable<Void>)t));
        check(CALLABLE, (c, t) -> c.callAsync((IgniteCallable<Void>)t).get());

        check(CALLABLE, (c, t) -> c.call((toList((IgniteCallable<Void>)t))));
        check(CALLABLE, (c, t) -> c.callAsync(toList((IgniteCallable<Void>)t)).get());

        check(CALLABLE, (c, t) -> c.call(toList((IgniteCallable<Void>)t), new TestReducer()));
        check(CALLABLE, (c, t) -> c.callAsync(toList((IgniteCallable<Void>)t), new TestReducer()).get());

        check(RUNNABLE, (c, t) -> c.run((IgniteRunnable)t));
        check(RUNNABLE, (c, t) -> c.runAsync((IgniteRunnable)t).get());

        check(RUNNABLE, (c, t) -> c.run(toList((IgniteRunnable)t)));
        check(RUNNABLE, (c, t) -> c.runAsync(toList((IgniteRunnable)t)).get());

        check(RUNNABLE, (c, t) -> c.broadcast((IgniteRunnable)t));
        check(RUNNABLE, (c, t) -> c.broadcastAsync((IgniteRunnable)t).get());

        check(CALLABLE, (c, t) -> c.broadcast((IgniteCallable<Void>)t));
        check(CALLABLE, (c, t) -> c.broadcastAsync((IgniteCallable<Void>)t).get());

        check(CLOSURE, ((c, t) -> c.broadcast((IgniteClosure<Void, Void>)t, null)));
        check(CLOSURE, (c, t) -> c.broadcastAsync((IgniteClosure<Void, Void>)t, null).get());

        check(CLOSURE, (c, t) -> c.apply((IgniteClosure<Void, Void>)t, (Void)null));
        check(CLOSURE, (c, t) -> c.applyAsync((IgniteClosure<Void, Void>)t, (Void)null).get());

        check(CLOSURE, (c, t) -> c.apply((IgniteClosure<Void, Void>)t, singletonList(null), new TestReducer()));
        check(CLOSURE, (c, t) -> c.applyAsync((IgniteClosure<Void, Void>)t, singletonList(null), new TestReducer()).get());

        check(RUNNABLE, (c, t) -> c.affinityRun(DEFAULT_CACHE_NAME, "key", (IgniteRunnable)t));
        check(RUNNABLE, (c, t) -> c.affinityRunAsync(DEFAULT_CACHE_NAME, "key", (IgniteRunnable)t).get());

        check(RUNNABLE, (c, t) -> c.affinityRun(singletonList(DEFAULT_CACHE_NAME), "key", (IgniteRunnable)t));
        check(RUNNABLE, (c, t) -> c.affinityRunAsync(singletonList(DEFAULT_CACHE_NAME), "key", (IgniteRunnable)t).get());

        check(RUNNABLE, (c, t) -> c.affinityRun(singletonList(DEFAULT_CACHE_NAME), 0, (IgniteRunnable)t));
        check(RUNNABLE, (c, t) -> c.affinityRunAsync(singletonList(DEFAULT_CACHE_NAME), 0, (IgniteRunnable)t).get());

        check(CALLABLE, (c, t) -> c.affinityCall(DEFAULT_CACHE_NAME, "key", (IgniteCallable<Void>)t));
        check(CALLABLE, (c, t) -> c.affinityCallAsync(DEFAULT_CACHE_NAME, "key", (IgniteCallable<Void>)t).get());

        check(CALLABLE, (c, t) -> c.affinityCall(singletonList(DEFAULT_CACHE_NAME), "key", (IgniteCallable<Void>)t));
        check(CALLABLE, (c, t) -> c.affinityCallAsync(singletonList(DEFAULT_CACHE_NAME), "key", (IgniteCallable<Void>)t).get());

        check(CALLABLE, (c, t) -> c.affinityCall(singletonList(DEFAULT_CACHE_NAME), 0, (IgniteCallable<Void>)t));
        check(CALLABLE, (c, t) -> c.affinityCallAsync(singletonList(DEFAULT_CACHE_NAME), 0, (IgniteCallable<Void>)t).get());
    }

    /** */
    public void check(ComputationType type, ConsumerX<Object> consumer) throws Exception {
        consumer.accept(grid().compute().withName(TEST_TASK_NAME), getComputationObject(type, TEST_TASK_NAME));
        consumer.accept(grid().compute(), getComputationObject(type, null));

        assertThrows(() -> consumer.accept(grid().compute().withName(TEST_TASK_NAME), null));
        consumer.accept(grid().compute(), getComputationObject(type, null));
    }

    /** */
    private static class TestCallable extends TaskNameChecker implements IgniteCallable<Void> {
        /** */
        public TestCallable(String expName) {
            super(expName);
        }

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            checkName();

            return null;
        }
    }

    /** */
    private static class TestRunnable extends TaskNameChecker implements IgniteRunnable {
        /** */
        public TestRunnable(String expName) {
            super(expName);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            checkName();
        }
    }

    /** */
    private static class TestClosure extends TaskNameChecker implements IgniteClosure<Void, Void> {
        /** */
        public TestClosure(String expName) {
            super(expName);
        }

        /** {@inheritDoc} */
        @Override public Void apply(Void arg) {
            checkName();

            return null;
        }
    }

    /** */
    private static class TestTask extends ComputeTaskAdapter<Void, Void> {
        /** */
        private final String name;

        /** */
        public TestTask(String name) {
            this.name = name == null ? getClass().getName() : name;
        }

        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            @Nullable Void arg
        ) throws IgniteException {
            return singletonMap(new TestJob(name), subgrid.iterator().next());
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List list) throws IgniteException {
            return null;
        }

        /** */
        private static class TestJob extends TaskNameChecker implements ComputeJob {
            /** */
            public TestJob(String expName) {
                super(expName);
            }

            /** {@inheritDoc} */
            @Override public Object execute() throws IgniteException {
                checkName();

                return null;
            }

            /** {@inheritDoc} */
            @Override public void cancel() {
                // No-op.
            }
        }
    }

    /** */
    private static class TaskNameChecker {
        /** */
        @TaskSessionResource
        private ComputeTaskSession ses;

        /** */
        private final String expName;

        /** */
        public TaskNameChecker(String expName) {
            this.expName = expName == null ? getClass().getName() : expName;
        }

        /** */
        protected void checkName() {
            assertEquals(expName, ses.getTaskName());
        }

    }

    /** */
    private void assertThrows(RunnableX r) {
        GridTestUtils.assertThrowsWithCause(r, Exception.class);
    }

    /** */
    private static class TestReducer implements IgniteReducer<Void, Void> {
        /** {@inheritDoc} */
        @Override public boolean collect(@Nullable Void o) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Void reduce() {
            return null;
        }
    }

    /** */
    private static <T> Collection<T> toList(T t) {
        return t == null ? null : singletonList(t);
    }

    /** */
    private interface ConsumerX<T> {
        /** */
        void accept(IgniteCompute c, T t) throws Exception;
    }

    /** */
    private Object getComputationObject(ComputationType type, String name) {
        switch (type) {
            case TASK: return new TestTask(name);
            case CLOSURE: return new TestClosure(name);
            case CALLABLE: return new TestCallable(name);
            case RUNNABLE: return new TestRunnable(name);
            default: throw new IllegalStateException();
        }
    }

    /** */
    enum ComputationType {
        /** */
        TASK,

        /** */
        CALLABLE,

        /** */
        RUNNABLE,

        /** */
        CLOSURE
    }
}
