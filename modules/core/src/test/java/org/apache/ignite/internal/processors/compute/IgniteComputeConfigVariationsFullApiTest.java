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

import org.apache.ignite.*;
import org.apache.ignite.binary.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.jdk.*;
import org.apache.ignite.testframework.configvariations.*;
import org.apache.ignite.testframework.junits.*;
import org.jetbrains.annotations.*;

import javax.cache.configuration.*;
import java.io.*;
import java.util.*;

/**
 * Full API compute test.
 */
public class IgniteComputeConfigVariationsFullApiTest extends IgniteConfigVariationsAbstractTest {
    /** Max job count. */
    private static final int MAX_JOB_COUNT = 10;

    /** Test cache name. */
    private static final String CACHE_NAME = "test";

    /** Job factories. */
    private static final Factory[] jobFactories = new Factory[] {
        Parameters.factory(EchoJob.class),
        Parameters.factory(EchoJobExternalizable.class),
        Parameters.factory(EchoJobBinarylizable.class)
    };

    /** Closure factories. */
    private static final Factory[] closureFactories = new Factory[] {
        Parameters.factory(EchoClosure.class),
        Parameters.factory(EchoClosureExternalizable.class),
        Parameters.factory(EchoClosureBinarylizable.class)
    };

    /** Callable factories. */
    private static final Factory[] callableFactories = new Factory[] {
        Parameters.factory(EchoCallable.class),
        Parameters.factory(EchoCallableExternalizable.class),
        Parameters.factory(EchoCallableBinarylizable.class)
    };

    /**
     * @param expCnt Expected count.
     * @param results Results.
     * @param dataCls Data class.
     */
    private static void checkResultsClassCount(final int expCnt, final Collection<Object> results,
        final Class dataCls) {
        int cnt = 0;
        for (Object o : results)
            if ((o != null) && dataCls.equals(o.getClass()))
                ++cnt;

        assertEquals("Count of the result objects' type mismatch (null values are filtered)", expCnt, cnt);
    }

    /**
     * @param expCnt Expected count.
     * @param results Results.
     */
    private static void checkNullCount(final int expCnt, final Collection<Object> results) {
        int cnt = 0;
        for (Object o : results)
            if (o == null)
                ++cnt;

        assertEquals("Count of the result objects' type mismatch (null values are filtered)", expCnt, cnt);

        assertEquals("Count of the null objects mismatch", expCnt, cnt);
    }

    /**
     * The test's wrapper runs the test with each factory from the factories array.
     *
     * @param test test object, a factory is passed as a parameter.
     * @param factories various factories
     */
    private void runWithAllFactories(ComputeTest test, Factory[] factories) throws Exception {
        for (int i = 0; i < factories.length; i++) {
            Factory factory = factories[i];

            info("Running test with jobs model: " + factory.create().getClass().getName());

            if (i != 0)
                beforeTest();

            try {
                test.test(factory);
            }
            finally {
                if (i + 1 != factories.length)
                    afterTest();
            }
        }
    }

    /**
     * The test's wrapper provides variations of the argument data model and user factories. The test is launched {@code
     * factories.length * DataMode.values().length} times.
     *
     * @param test Test.
     * @param factories various factories
     */
    protected void runTest(final Factory[] factories, final ComputeTest test) throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                try {
                    if ((getConfiguration().getMarshaller() instanceof JdkMarshaller)
                        && (dataMode == DataMode.PLANE_OBJECT)) {
                        info("Skip test for JdkMarshaller & PLANE_OBJECT data mode");
                        return;
                    }
                }
                catch (Exception e) {
                    assert false : e.getMessage();
                }

                runWithAllFactories(test, factories);
            }
        });
    }

    /**
     */
    public void testExecuteTaskClass() throws Exception {
        runTest(jobFactories, new ComputeTest() {
            @Override public void test(Factory factory) throws Exception {
                // begin with negative to check 'null' value in the test
                final int[] i = {-1};

                List<Object> results = grid(0).compute().execute(
                    TestTask.class,
                    new T2<>((Factory<ComputeJobAdapter>)factory,
                        (Factory<Object>)new Factory<Object>() {
                        @Override public Object create() {
                            return value(i[0]++);
                        }
                    }));

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                checkNullCount(1, results);
            }
        });
    }

    /**
     */
    public void testExecuteTask() throws Exception {
        runTest(jobFactories, new ComputeTest() {
            @Override public void test(Factory factory) throws Exception {
                // begin with negative to check 'null' value in the test
                final int[] i = {-1};

                List<Object> results = grid(0).compute().execute(new TestTask(),
                    new T2<>((Factory<ComputeJobAdapter>)factory,
                        (Factory<Object>)new Factory<Object>() {
                        @Override public Object create() {
                            return value(i[0]++);
                        }
                    }));

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                checkNullCount(1, results);
            }
        });
    }

    /**
     */
    public void testBroadcast() throws Exception {
        runTest(closureFactories, new ComputeTest() {
            @Override public void test(Factory factory) throws Exception {
                final IgniteCompute comp = grid(0).compute();
                final Collection<Object> resultsAllNull = comp.broadcast((IgniteClosure<Object, Object>)factory.create(), null);

                assertEquals("Result's size mismatch", gridCount(), resultsAllNull.size());
                for (Object o : resultsAllNull)
                    assertNull("All results must be null", o);

                Collection<Object> resultsNotNull = grid(0).compute()
                    .broadcast((IgniteClosure<Object, Object>)factory.create(), value(0));

                checkResultsClassCount(gridCount(), resultsNotNull, value(0).getClass());
            }
        });
    }

    /**
     */
    public void testApplyAsync() throws Exception {
        runTest(closureFactories, new ComputeTest() {
            @Override public void test(Factory factory) throws Exception {
                final IgniteCompute comp = grid(0).compute().withAsync();

                List<ComputeTaskFuture<Object>> futures = new ArrayList<>(MAX_JOB_COUNT);
                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    // value(i - 1): use negative argument of the value method to generate null value
                    comp.apply((IgniteClosure<Object, Object>)factory.create(), value(i - 1));
                    futures.add(comp.future());
                }

                // wait for results
                Collection<Object> results = new ArrayList<Object>(MAX_JOB_COUNT);
                for (ComputeTaskFuture<Object> future : futures)
                    results.add(future.get());

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                checkNullCount(1, results);
            }
        });
    }

    /**
     */
    public void testApplySync() throws Exception {
        runTest(closureFactories, new ComputeTest() {
            @Override public void test(Factory factory) throws Exception {
                final IgniteCompute comp = grid(0).compute();

                Collection<Object> results = new ArrayList<Object>(MAX_JOB_COUNT);
                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    // value(i - 1): use negative argument of the value method to generate null value
                    results.add(comp.apply((IgniteClosure<Object, Object>)factory.create(), value(i - 1)));
                }

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                checkNullCount(1, results);
            }
        });
    }

    /**
     */
    public void testCallAsync() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory) throws Exception {
                final IgniteCompute comp = grid(0).compute().withAsync();

                List<ComputeTaskFuture<Object>> futures = new ArrayList<ComputeTaskFuture<Object>>(MAX_JOB_COUNT);
                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    EchoCallable job = (EchoCallable)factory.create();
                    job.setArg(value(i - 1));

                    comp.call(job);
                    futures.add(comp.future());
                }

                // wait for results
                Collection<Object> results = new ArrayList<Object>(MAX_JOB_COUNT);
                for (ComputeTaskFuture<Object> future : futures)
                    results.add(future.get());

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                checkNullCount(1, results);
            }
        });
    }

    /**
     */
    public void testCallSync() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory) throws Exception {
                final IgniteCompute comp = grid(0).compute();

                Collection<Object> results = new ArrayList<>(MAX_JOB_COUNT);
                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    EchoCallable job = (EchoCallable)factory.create();
                    job.setArg(value(i - 1));
                    results.add(comp.call(job));
                }

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                checkNullCount(1, results);
            }
        });
    }

    /**
     */
    public void testCallCollection() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory) throws Exception {
                IgniteCompute comp = grid(0).compute();

                List<EchoCallable> jobs = new ArrayList<EchoCallable>(MAX_JOB_COUNT);
                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    EchoCallable job = (EchoCallable)factory.create();
                    job.setArg(value(i - 1));
                    jobs.add(job);
                }

                Collection<Object> results = comp.call(jobs);

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                checkNullCount(1, results);
            }
        });
    }

    /**
     */
    public void testDummyAffinityCall() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory) throws Exception {
                grid(0).getOrCreateCache(CACHE_NAME);
                grid(0).cache(CACHE_NAME).putIfAbsent(key(0), value(0));

                final IgniteCompute comp = grid(0).compute();

                Collection<Object> results = new ArrayList<>(MAX_JOB_COUNT);
                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    EchoCallable job = (EchoCallable)factory.create();
                    job.setArg(value(i - 1));

                    results.add(comp.affinityCall("test", key(0), job));
                }

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                checkNullCount(1, results);
            }
        });
    }

    /**
     * Override the base method to return {@code null} value in case the valId is negative.
     */
    @Nullable @Override public Object value(int valId) {
        if (valId < 0)
            return null;
        return super.value(valId);
    }

    /**
     *
     */
    public static interface ComputeTest {
        /**
         * @throws Exception If failed.
         */
        public void test(Factory factory) throws Exception;
    }

    /**
     * Creates set of jobs.
     */
    @SuppressWarnings({"PublicInnerClass"})
    static class TestTask
        extends ComputeTaskSplitAdapter<T2<Factory<ComputeJobAdapter>, Factory<Object>>, List<Object>> {
        /**
         * {@inheritDoc}
         */
        @Override protected Collection<? extends ComputeJob> split(int gridSize,
            T2<Factory<ComputeJobAdapter>, Factory<Object>> factoriesJobAndArg) throws IgniteException {
            Collection<ComputeJob> jobs = new HashSet<>();

            for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                ComputeJobAdapter job = factoriesJobAndArg.get1().create();
                job.setArguments(factoriesJobAndArg.get2().create());
                jobs.add(job);
            }
            return jobs;
        }

        /**
         * {@inheritDoc}
         */
        @Nullable @Override public List<Object> reduce(List<ComputeJobResult> results) throws IgniteException {
            List<Object> ret = new ArrayList<>(results.size());
            for(ComputeJobResult result : results)
                ret.add(result.getData());

            return ret;
        }
    }

    /**
     * Echo job, serializable object
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoJob extends ComputeJobAdapter {

        /**
         * Default constructor (required by ReflectionFactory).
         */
        public EchoJob() {
        }

        /**
         * {@inheritDoc}
         */
        @Nullable @Override public Object execute() {
            System.out.println((argument(0) == null) ? "null" : argument(0).toString());
            return argument(0);
        }
    }

    /**
     * Echo job, externalizable
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoJobExternalizable extends EchoJob implements Externalizable {

        /**
         * Default constructor (required by Externalizable).
         */
        public EchoJobExternalizable() {
        }

        /**
         * {@inheritDoc}
         */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(argument(0));
        }

        /**
         * {@inheritDoc}
         */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            setArguments(in.readObject());
        }
    }

    /**
     * Echo job, externalizable
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoJobBinarylizable extends EchoJob implements Binarylizable {

        /**
         * Default constructor (required by ReflectionFactory).
         */
        public EchoJobBinarylizable() {
        }

        /**
         * {@inheritDoc}
         */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeObject("arg", argument(0));
        }

        /**
         * {@inheritDoc}
         */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            Object arg = reader.readObject("arg");

            setArguments(arg);
        }
    }

    /**
     * Echo job, serializable object
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoClosure implements IgniteClosure<Object, Object> {

        /** {@inheritDoc} */
        @Override public Object apply(Object arg) {
            System.out.println((arg == null) ? "null" : arg.toString());
            return arg;
        }
    }

    /**
     * Echo closure, externalizable
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoClosureExternalizable extends EchoClosure implements Externalizable {

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        }
    }

    /**
     * Echo closure, externalizable
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoClosureBinarylizable extends EchoClosure implements Binarylizable {

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        }
    }

    /**
     * Test callable, serializable object
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoCallable implements IgniteCallable<Object> {
        /** */
        protected Object arg;

        /**
         */
        public EchoCallable() {
        }

        /**
         * @param arg Argument.
         */
        void setArg(@Nullable Object arg) {
            this.arg = arg;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object call() throws Exception {
            System.out.println((arg == null) ? "null" : arg.toString());
            return arg;
        }
    }

    /**
     * Echo callable, externalizable object
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoCallableExternalizable extends EchoCallable implements Externalizable {
        /**
         * Default constructor.
         */
        public EchoCallableExternalizable() {
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(arg);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            arg = in.readObject();
        }
    }

    /**
     * Echo callable, binarylizable object
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoCallableBinarylizable extends EchoCallable implements Binarylizable {

        /**
         * Default constructor.
         */
        public EchoCallableBinarylizable() {

        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeObject("arg", arg);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            arg = reader.readObject("arg");
        }
    }

}
