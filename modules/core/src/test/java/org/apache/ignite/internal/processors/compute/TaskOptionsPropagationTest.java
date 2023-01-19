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
import org.apache.ignite.IgniteCompute;
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
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.singletonList;
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
    public void testTaskExecutioOptionsReset() throws Exception {
        checkCallable(IgniteCompute::call);
        checkCallable((c, t) -> c.callAsync(t).get());

        checkCallable((c, t) -> c.call((toList(t))));
        checkCallable((c, t) -> c.callAsync(toList(t)).get());

        checkCallable((c, t) -> c.call(toList(t), new TestReducer()));
        checkCallable((c, t) -> c.callAsync(toList(t), new TestReducer()).get());

        checkRunnable(IgniteCompute::run);
        checkRunnable((c, t) -> c.runAsync(t).get());

        checkRunnable((c, t) -> c.run(toList(t)));
        checkRunnable((c, t) -> c.runAsync(toList(t)).get());

        checkRunnable(IgniteCompute::broadcast);
        checkRunnable((c, t) -> c.broadcastAsync(t).get());

        checkCallable(IgniteCompute::broadcast);
        checkCallable((c, t) -> c.broadcastAsync(t).get());

        checkClosure((c, t) -> c.broadcast(t, null));
        checkClosure((c, t) -> c.broadcastAsync(t, null).get());

        checkClosure((c, t) -> c.apply(t, (Void)null));
        checkClosure((c, t) -> c.applyAsync(t, (Void)null).get());

        checkClosure((c, t) -> c.apply(t, singletonList(null), new TestReducer()));
        checkClosure((c, t) -> c.applyAsync(t, singletonList(null), new TestReducer()).get());

        checkRunnable((c, t) -> c.affinityRun(DEFAULT_CACHE_NAME, "key", t));
        checkRunnable((c, t) -> c.affinityRunAsync(DEFAULT_CACHE_NAME, "key", t).get());

        checkRunnable((c, t) -> c.affinityRun(singletonList(DEFAULT_CACHE_NAME), "key", t));
        checkRunnable((c, t) -> c.affinityRunAsync(singletonList(DEFAULT_CACHE_NAME), "key", t).get());

        checkRunnable((c, t) -> c.affinityRun(singletonList(DEFAULT_CACHE_NAME), 0, t));
        checkRunnable((c, t) -> c.affinityRunAsync(singletonList(DEFAULT_CACHE_NAME), 0, t).get());

        checkCallable((c, t) -> c.affinityCall(DEFAULT_CACHE_NAME, "key", t));
        checkCallable((c, t) -> c.affinityCallAsync(DEFAULT_CACHE_NAME, "key", t).get());

        checkCallable((c, t) -> c.affinityCall(singletonList(DEFAULT_CACHE_NAME), "key", t));
        checkCallable((c, t) -> c.affinityCallAsync(singletonList(DEFAULT_CACHE_NAME), "key", t).get());

        checkCallable((c, t) -> c.affinityCall(singletonList(DEFAULT_CACHE_NAME), 0, t));
        checkCallable((c, t) -> c.affinityCallAsync(singletonList(DEFAULT_CACHE_NAME), 0, t).get());
    }

    /** */
    public void checkCallable(ConsumerX<IgniteCallable<Void>> consumer) throws Exception {
        consumer.accept(grid().compute().withName(TEST_TASK_NAME), new TestCallable(TEST_TASK_NAME));
        consumer.accept(grid().compute(), new TestCallable(TestCallable.class.getName()));

        assertThrows(() -> consumer.accept(grid().compute().withName(TEST_TASK_NAME), null));
        consumer.accept(grid().compute(), new TestCallable(TestCallable.class.getName()));
    }

    /** */
    public void checkRunnable(ConsumerX<IgniteRunnable> consumer) throws Exception {
        consumer.accept(grid().compute().withName(TEST_TASK_NAME), new TestRunnable(TEST_TASK_NAME));
        consumer.accept(grid().compute(), new TestRunnable(TestRunnable.class.getName()));

        assertThrows(() -> consumer.accept(grid().compute().withName(TEST_TASK_NAME), null));
        consumer.accept(grid().compute(), new TestRunnable(TestRunnable.class.getName()));
    }

    /** */
    public void checkClosure(ConsumerX<IgniteClosure<Void, Void>> consumer) throws Exception {
        consumer.accept(grid().compute().withName(TEST_TASK_NAME), new TestClosure(TEST_TASK_NAME));
        consumer.accept(grid().compute(), new TestClosure(TestClosure.class.getName()));

        assertThrows(() -> consumer.accept(grid().compute().withName(TEST_TASK_NAME), null));
        consumer.accept(grid().compute(), new TestClosure(TestClosure.class.getName()));
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
    private static class TaskNameChecker {
        /** */
        @TaskSessionResource
        private ComputeTaskSession ses;

        /** */
        private final String expName;

        /** */
        public TaskNameChecker(String expName) {
            this.expName = expName;
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
}
