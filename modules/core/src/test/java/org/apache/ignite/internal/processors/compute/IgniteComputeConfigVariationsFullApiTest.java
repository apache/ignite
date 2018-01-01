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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

/**
 * Full API compute test.
 */
@SuppressWarnings("unchecked")
public class IgniteComputeConfigVariationsFullApiTest extends IgniteConfigVariationsAbstractTest {
    /** Max job count. */
    private static final int MAX_JOB_COUNT = 10;

    /** Test cache name. */
    private static final String CACHE_NAME = "test";

    /** */
    private static final String STR_VAL = "string value";

    /** */
    private static final Object[] ARRAY_VAL = {"str0", "str1", "str2"};

    /** Job factories. */
    private static final JobFactory[] jobFactories = new JobFactory[] {
        new JobFactory(EchoJob.class),
        new JobFactory(EchoJobExternalizable.class),
        new JobFactory(EchoJobBinarylizable.class)
    };

    /** Closure factories. */
    private static final Factory[] closureFactories = new Factory[] {
        new JobFactory(EchoClosure.class),
        new JobFactory(EchoClosureExternalizable.class),
        new JobFactory(EchoClosureBinarylizable.class)
    };

    /** Callable factories. */
    private static final Factory[] callableFactories = new Factory[] {
        new JobFactory(EchoCallable.class),
        new JobFactory(EchoCallableExternalizable.class),
        new JobFactory(EchoCallableBinarylizable.class)
    };

    /** Runnable factories. */
    private static final Factory[] runnableFactories = new Factory[] {
        new JobFactory(ComputeTestRunnable.class),
        new JobFactory(ComputeTestRunnableExternalizable.class),
        new JobFactory(ComputeTestRunnableBinarylizable.class)
    };

    /**
     * @param expCnt Expected count.
     * @param results Results.
     * @param dataCls Data class.
     */
    private static void checkResultsClassCount(final int expCnt, final Collection<Object> results,
        final Class dataCls) {
        int cnt = 0;

        for (Object o : results) {
            if ((o != null) && dataCls.equals(o.getClass()))
                ++cnt;
        }

        assertEquals("Count of the result objects' type mismatch (null values are filtered)", expCnt, cnt);
    }

    /**
     * @return Expected valid result.
     */
    private Collection<Object> createGoldenResults() {
        Collection<Object> results = new ArrayList<>(MAX_JOB_COUNT);

        for (int i = 0; i < MAX_JOB_COUNT; ++i)
            results.add(value(i - 1));

        return results;
    }

    /**
     * @param msg Message.
     * @param exp Expected.
     * @param act Action.
     */
    private <E> void assertCollectionsEquals(String msg, Collection<E> exp, Collection<E> act) {
        assertEquals(msg + "; Size are different", exp.size(), act.size());

        for (Object o : exp) {
            if (!act.contains(o)) {
                error("Expected: " + exp.toString());
                error("Actual: " + act.toString());

                assertTrue(msg + String.format("; actual collection doesn't contain the object [%s]", o), false);
            }
        }
    }

    /**
     * The test's wrapper provides variations of the argument data model and user factories. The test is launched {@code
     * factories.length * DataMode.values().length} times with each datamodel and each factory.
     *
     * @param test Test.
     * @param factories various factories
     * @throws Exception If failed.
     */
    protected void runTest(final Factory[] factories, final ComputeTest test) throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                for (int i = 0; i < factories.length; i++) {
                    Factory factory = factories[i];

                    info("Running test with jobs model: " + factory.create().getClass().getSimpleName());

                    if (i != 0)
                        beforeTest();

                    try {
                        test.test(factory, grid(testedNodeIdx));
                    }
                    finally {
                        if (i + 1 != factories.length)
                            afterTest();
                    }
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteTaskClass() throws Exception {
        runTest(jobFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                // Begin with negative to check 'null' value in the test.
                final int[] i = {-1};

                List<Object> results = ignite.compute().execute(
                    TestTask.class,
                    new T2<>((Factory<ComputeJobAdapter>)factory,
                        (Factory<Object>)new Factory<Object>() {
                            @Override public Object create() {
                                return value(i[0]++);
                            }
                        }));

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteTaskClassAsync() throws Exception {
        runTest(jobFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                // Begin with negative to check 'null' value in the test.
                final int[] i = {-1};

                ComputeTaskFuture<List<Object>> fut = ignite.compute().executeAsync(
                    TestTask.class,
                    new T2<>((Factory<ComputeJobAdapter>)factory,
                        (Factory<Object>)new Factory<Object>() {
                            @Override public Object create() {
                                return value(i[0]++);
                            }
                        }));

                checkResultsClassCount(MAX_JOB_COUNT - 1, fut.get(), value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), fut.get());
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteTask() throws Exception {
        runTest(jobFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                // Begin with negative to check 'null' value in the test.
                final int[] i = {-1};

                List<Object> results = ignite.compute().execute(new TestTask(),
                    new T2<>((Factory<ComputeJobAdapter>)factory,
                        (Factory<Object>)new Factory<Object>() {
                            @Override public Object create() {
                                return value(i[0]++);
                            }
                        }));

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteTaskAsync() throws Exception {
        runTest(jobFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                // Begin with negative to check 'null' value in the test.
                final int[] i = {-1};

                ComputeTaskFuture<List<Object>> fut = ignite.compute().executeAsync(new TestTask(),
                    new T2<>((Factory<ComputeJobAdapter>)factory,
                        (Factory<Object>)new Factory<Object>() {
                            @Override public Object create() {
                                return value(i[0]++);
                            }
                        }));

                checkResultsClassCount(MAX_JOB_COUNT - 1, fut.get(), value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), fut.get());
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testBroadcastClosure() throws Exception {
        runTest(closureFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                final Collection<Object> resultsAllNull = ignite.compute()
                    .broadcast((IgniteClosure<Object, Object>)factory.create(), null);

                assertEquals("Result's size mismatch: job must be run on all server nodes",
                    gridCount() - clientsCount(), resultsAllNull.size());

                for (Object o : resultsAllNull)
                    assertNull("All results must be null", o);

                Collection<Object> resultsNotNull = ignite.compute()
                    .broadcast((IgniteClosure<Object, Object>)factory.create(), value(0));

                checkResultsClassCount(gridCount() - clientsCount(), resultsNotNull, value(0).getClass());
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testBroadcastClosureAsync() throws Exception {
        runTest(closureFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                final Collection<Object> resultsAllNull = ignite.compute()
                    .broadcast((IgniteClosure<Object, Object>)factory.create(), null);

                assertEquals("Result's size mismatch: job must be run on all server nodes",
                    gridCount() - clientsCount(), resultsAllNull.size());

                for (Object o : resultsAllNull)
                    assertNull("All results must be null", o);

                IgniteFuture<Collection<Object>> fut = ignite.compute()
                    .broadcastAsync((IgniteClosure<Object, Object>)factory.create(), value(0));

                checkResultsClassCount(gridCount() - clientsCount(), fut.get(), value(0).getClass());
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testBroadcastCallable() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                EchoCallable job = (EchoCallable)factory.create();
                job.setArg(null);

                final Collection<Object> resultsAllNull = ignite.compute()
                    .broadcast(job);

                assertEquals("Result's size mismatch: job must be run on all server nodes",
                    gridCount() - clientsCount(), resultsAllNull.size());

                for (Object o : resultsAllNull)
                    assertNull("All results must be null", o);

                job.setArg(value(0));
                Collection<Object> resultsNotNull = ignite.compute()
                    .broadcast(job);

                checkResultsClassCount(gridCount() - clientsCount(), resultsNotNull, value(0).getClass());
                for (Object o : resultsNotNull)
                    assertEquals("Invalid broadcast results", value(0), o);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testBroadcastCallableAsync() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                EchoCallable job = (EchoCallable)factory.create();
                job.setArg(null);

                final IgniteFuture<Collection<Object>> futAllNull = ignite.compute()
                    .broadcastAsync(job);

                assertEquals("Result's size mismatch: job must be run on all server nodes",
                    gridCount() - clientsCount(), futAllNull.get().size());

                for (Object o : futAllNull.get())
                    assertNull("All results must be null", o);

                job.setArg(value(0));
                IgniteFuture<Collection<Object>> futNotNull = ignite.compute()
                    .broadcastAsync(job);

                checkResultsClassCount(gridCount() - clientsCount(), futNotNull.get(), value(0).getClass());
                for (Object o : futNotNull.get())
                    assertEquals("Invalid broadcast results", value(0), o);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testBroadcastRunnable() throws Exception {
        runTest(runnableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                IgniteRunnable job = (IgniteRunnable)factory.create();

                ignite.compute().broadcast(job);
                // All checks are inside the run() method of the job.
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testBroadcastRunnableAsync() throws Exception {
        runTest(runnableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                IgniteRunnable job = (IgniteRunnable)factory.create();

                IgniteFuture<Void> fut = ignite.compute().broadcastAsync(job);

                fut.get();
                // All checks are inside the run() method of the job.
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testRun() throws Exception {
        runTest(runnableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                IgniteRunnable job = (IgniteRunnable)factory.create();

                ignite.compute().run(job);
                // All checks are inside the run() method of the job.

                Collection<IgniteRunnable> jobs = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i)
                    jobs.add((IgniteRunnable)factory.create());

                ignite.compute().run(jobs);
                // All checks are inside the run() method of the job.
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testRunAsync() throws Exception {
        runTest(runnableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                IgniteRunnable job = (IgniteRunnable)factory.create();

                IgniteFuture<Void> fut0 = ignite.compute().runAsync(job);

                fut0.get();
                // All checks are inside the run() method of the job.

                Collection<IgniteRunnable> jobs = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i)
                    jobs.add((IgniteRunnable)factory.create());

                IgniteFuture<Void> fut1 = ignite.compute().runAsync(jobs);

                fut1.get();
                // All checks are inside the run() method of the job.
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testApplyAsync() throws Exception {
        runTest(closureFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                final IgniteCompute comp = ignite.compute();

                Collection<IgniteFuture<Object>> futures = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    // value(i - 1): use negative argument of the value method to generate nullong value.
                    futures.add(comp.applyAsync((IgniteClosure<Object, Object>)factory.create(), value(i - 1)));
                }

                // Wait for results.
                Collection<Object> results = new ArrayList<>(MAX_JOB_COUNT);

                for (IgniteFuture<Object> future : futures)
                    results.add(future.get());

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testApply() throws Exception {
        runTest(closureFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                Collection<Object> results = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    // value(i - 1): use negative argument of the value method to generate nullong value.
                    results.add(ignite.compute().apply((IgniteClosure<Object, Object>)factory.create(), value(i - 1)));
                }

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testApplyForCollection() throws Exception {
        runTest(closureFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                Collection params = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    // value(i - 1): use negative argument of the value method to generate nullong value.
                    params.add(value(i - 1));
                }

                IgniteClosure c = (IgniteClosure)factory.create();

                // Use type casting to avoid ambiguous for apply(Callable, Object) vs apply(Callable, Collection<Object>).
                Collection<Object> results = ignite.compute().apply((IgniteClosure<TestObject, Object>)c,
                    (Collection<TestObject>)params);

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testApplyForCollectionAsync() throws Exception {
        runTest(closureFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                Collection params = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    // value(i - 1): use negative argument of the value method to generate nullong value.
                    params.add(value(i - 1));
                }

                IgniteClosure c = (IgniteClosure)factory.create();

                // Use type casting to avoid ambiguous for apply(Callable, Object) vs apply(Callable, Collection<Object>).
                IgniteFuture<Collection<Object>> fut = ignite.compute().applyAsync(
                    (IgniteClosure<TestObject, Object>)c,
                    (Collection<TestObject>)params);

                checkResultsClassCount(MAX_JOB_COUNT - 1, fut.get(), value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), fut.get());
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testApplyForCollectionWithReducer() throws Exception {
        runTest(closureFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                Collection<Object> params = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    // value(i - 1): use negative argument of the value method to generate nullong value.
                    params.add(value(i - 1));
                }

                boolean res = ignite.compute()
                    .apply((IgniteClosure<Object, Object>)factory.create(), params, new IgniteReducer<Object, Boolean>() {

                        private Collection<Object> results = new ArrayList<>(MAX_JOB_COUNT);

                        @Override public boolean collect(@Nullable Object o) {
                            results.add(o);
                            return true;
                        }

                        @Override public Boolean reduce() {
                            checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                            assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
                            return true;
                        }
                    });

                assertTrue(res);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testApplyForCollectionWithReducerAsync() throws Exception {
        runTest(closureFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                Collection<Object> params = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    // value(i - 1): use negative argument of the value method to generate nullong value.
                    params.add(value(i - 1));
                }

                IgniteFuture<Boolean> fut = ignite.compute()
                    .applyAsync((IgniteClosure<Object, Object>)factory.create(), params, new IgniteReducer<Object, Boolean>() {

                        private Collection<Object> results = new ArrayList<>(MAX_JOB_COUNT);

                        @Override public boolean collect(@Nullable Object o) {
                            results.add(o);
                            return true;
                        }

                        @Override public Boolean reduce() {
                            checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                            assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
                            return true;
                        }
                    });

                assertTrue(fut.get());
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallAsync() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                final IgniteCompute comp = ignite.compute();

                Collection<IgniteFuture<Object>> futures = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    EchoCallable job = (EchoCallable)factory.create();
                    job.setArg(value(i - 1));

                    futures.add(comp.callAsync(job));
                }

                // Wait for results.
                Collection<Object> results = new ArrayList<>(MAX_JOB_COUNT);
                for (IgniteFuture<Object> future : futures)
                    results.add(future.get());

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCall() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                Collection<Object> results = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    EchoCallable job = (EchoCallable)factory.create();
                    job.setArg(value(i - 1));
                    results.add(ignite.compute().call(job));
                }

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallCollection() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                Collection<EchoCallable> jobs = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    EchoCallable job = (EchoCallable)factory.create();
                    job.setArg(value(i - 1));
                    jobs.add(job);
                }

                Collection<Object> results = ignite.compute().call(jobs);

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallCollectionAsync() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                Collection<EchoCallable> jobs = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    EchoCallable job = (EchoCallable)factory.create();
                    job.setArg(value(i - 1));
                    jobs.add(job);
                }

                IgniteFuture<Collection<Object>> fut = ignite.compute().callAsync(jobs);

                checkResultsClassCount(MAX_JOB_COUNT - 1, fut.get(), value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), fut.get());
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallCollectionWithReducer() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                Collection<EchoCallable> jobs = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    EchoCallable job = (EchoCallable)factory.create();
                    job.setArg(value(i - 1));
                    jobs.add(job);
                }

                boolean res = ignite.compute().call(jobs, new IgniteReducer<Object, Boolean>() {
                    private Collection<Object> results = new ArrayList<>(MAX_JOB_COUNT);

                    @Override public boolean collect(@Nullable Object o) {
                        results.add(o);
                        return true;
                    }

                    @Override public Boolean reduce() {
                        checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                        assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
                        return true;
                    }
                });

                assertTrue(res);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallCollectionWithReducerAsync() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                Collection<EchoCallable> jobs = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    EchoCallable job = (EchoCallable)factory.create();
                    job.setArg(value(i - 1));
                    jobs.add(job);
                }

                IgniteFuture<Boolean> fut = ignite.compute().callAsync(jobs, new IgniteReducer<Object, Boolean>() {
                    private Collection<Object> results = new ArrayList<>(MAX_JOB_COUNT);

                    @Override public boolean collect(@Nullable Object o) {
                        results.add(o);
                        return true;
                    }

                    @Override public Boolean reduce() {
                        checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                        assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
                        return true;
                    }
                });

                assertTrue(fut.get());
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityCall() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                ignite.getOrCreateCache(CACHE_NAME);

                final IgniteCompute comp = ignite.compute();

                Collection<Object> results = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    EchoCallable job = (EchoCallable)factory.create();

                    job.setArg(value(i - 1));

                    results.add(comp.affinityCall("test", key(0), job));
                }

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityCallAsync() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                ignite.getOrCreateCache(CACHE_NAME);

                final IgniteCompute comp = ignite.compute();

                Collection<Object> results = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    EchoCallable job = (EchoCallable)factory.create();

                    job.setArg(value(i - 1));

                    IgniteFuture<Object> fut = comp.affinityCallAsync("test", key(0), job);

                    results.add(fut.get());
                }

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiCacheAffinityCall() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                ignite.getOrCreateCache("test0");
                ignite.getOrCreateCache("test1");

                final IgniteCompute comp = ignite.compute();

                Collection<Object> results = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    EchoCallable job = (EchoCallable)factory.create();

                    job.setArg(value(i - 1));

                    results.add(comp.affinityCall(Arrays.asList("test0", "test1"), key(0), job));
                }

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiCacheAffinityCallAsync() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                ignite.getOrCreateCache("test0");
                ignite.getOrCreateCache("test1");

                final IgniteCompute comp = ignite.compute();

                Collection<Object> results = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    EchoCallable job = (EchoCallable)factory.create();

                    job.setArg(value(i - 1));

                    IgniteFuture<Object> fut = comp.affinityCallAsync(Arrays.asList("test0", "test1"), key(0), job);

                    results.add(fut.get());
                }

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiCacheByPartIdAffinityCall() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                ignite.getOrCreateCache("test0");
                ignite.getOrCreateCache("test1");

                final IgniteCompute comp = ignite.compute();

                Collection<Object> results = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    EchoCallable job = (EchoCallable)factory.create();

                    job.setArg(value(i - 1));

                    results.add(comp.affinityCall(Arrays.asList("test0", "test1"), 0, job));
                }

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiCacheByPartIdAffinityCallAsync() throws Exception {
        runTest(callableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                ignite.getOrCreateCache("test0");
                ignite.getOrCreateCache("test1");

                final IgniteCompute comp = ignite.compute();

                Collection<Object> results = new ArrayList<>(MAX_JOB_COUNT);

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    EchoCallable job = (EchoCallable)factory.create();

                    job.setArg(value(i - 1));

                    IgniteFuture fut = comp.affinityCallAsync(Arrays.asList("test0", "test1"), 0, job);

                    results.add(fut.get());
                }

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityRun() throws Exception {
        runTest(runnableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                ignite.getOrCreateCache(CACHE_NAME);

                final IgniteCompute comp = ignite.compute();

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    IgniteRunnable job = (IgniteRunnable)factory.create();

                    comp.affinityRun("test", key(0), job);
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityRunAsync() throws Exception {
        runTest(runnableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                ignite.getOrCreateCache(CACHE_NAME);

                final IgniteCompute comp = ignite.compute();

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    IgniteRunnable job = (IgniteRunnable)factory.create();

                    IgniteFuture<Void> fut = comp.affinityRunAsync("test", key(0), job);

                    fut.get();
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiCacheAffinityRun() throws Exception {
        runTest(runnableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                ignite.getOrCreateCache("test0");
                ignite.getOrCreateCache("test1");

                final IgniteCompute comp = ignite.compute();

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    IgniteRunnable job = (IgniteRunnable)factory.create();

                    comp.affinityRun(Arrays.asList("test0", "test1"), key(0), job);
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiCacheAffinityRunAsync() throws Exception {
        runTest(runnableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                ignite.getOrCreateCache("test0");
                ignite.getOrCreateCache("test1");

                final IgniteCompute comp = ignite.compute();

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    IgniteRunnable job = (IgniteRunnable)factory.create();

                    IgniteFuture<Void> fut = comp.affinityRunAsync(Arrays.asList("test0", "test1"), key(0), job);

                    fut.get();
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiCacheByPartIdAffinityRun() throws Exception {
        runTest(runnableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                ignite.getOrCreateCache("test0");
                ignite.getOrCreateCache("test1");

                final IgniteCompute comp = ignite.compute();

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    IgniteRunnable job = (IgniteRunnable)factory.create();

                    comp.affinityRun(Arrays.asList("test0", "test1"), 0, job);
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiCacheByPartIdAffinityRunAsync() throws Exception {
        runTest(runnableFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                ignite.getOrCreateCache("test0");
                ignite.getOrCreateCache("test1");

                final IgniteCompute comp = ignite.compute();

                for (int i = 0; i < MAX_JOB_COUNT; ++i) {
                    IgniteRunnable job = (IgniteRunnable)factory.create();

                    IgniteFuture<Void> fut = comp.affinityRunAsync(Arrays.asList("test0", "test1"), 0, job);

                    fut.get();
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployExecuteByName() throws Exception {
        runTest(jobFactories, new ComputeTest() {
            @Override public void test(Factory factory, Ignite ignite) throws Exception {
                final int[] i = {-1};

                final IgniteCompute comp = ignite.compute();

                comp.localDeployTask(TestTask.class, TestTask.class.getClassLoader());

                List<Object> results = ignite.compute().execute(
                    TestTask.class.getName(),
                    new T2<>((Factory<ComputeJobAdapter>)factory,
                        (Factory<Object>)new Factory<Object>() {
                            @Override public Object create() {
                                return value(i[0]++);
                            }
                        }));

                checkResultsClassCount(MAX_JOB_COUNT - 1, results, value(0).getClass());
                assertCollectionsEquals("Results value mismatch", createGoldenResults(), results);

                comp.undeployTask(TestTask.class.getName());
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
    enum TestJobEnum {
        /** */
        VALUE_0,
        /** */
        VALUE_1,
        /** */
        VALUE_2
    }

    /**
     *
     */
    public interface ComputeTest {
        /**
         * @param factory Factory.
         * @param ignite Ignite instance to use.
         * @throws Exception If failed.
         */
        public void test(Factory factory, Ignite ignite) throws Exception;
    }

    /**
     * Creates set of jobs.
     */
    static class TestTask
        extends ComputeTaskSplitAdapter<T2<Factory<ComputeJobAdapter>, Factory<Object>>, List<Object>> {
        /** {@inheritDoc} */
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

        /** {@inheritDoc} */
        @Nullable @Override public List<Object> reduce(List<ComputeJobResult> results) throws IgniteException {
            List<Object> ret = new ArrayList<>(results.size());

            for (ComputeJobResult result : results)
                ret.add(result.getData());

            return ret;
        }
    }

    /**
     * Echo job, serializable object. All fields are used only for serialization check.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoJob extends ComputeJobAdapter {
        /** */
        private boolean isVal;

        /** */
        private byte bVal;

        /** */
        private char cVal;

        /** */
        private short sVal;

        /** */
        private int intVal;

        /** */
        private long lVal;

        /** */
        private float fltVal;

        /** */
        private double dblVal;

        /** */
        private String strVal;

        /** */
        private Object[] arrVal;

        /** */
        private TestJobEnum eVal;

        /**
         * Default constructor (required by ReflectionFactory).
         */
        public EchoJob() {
            // No-op.
        }

        /**
         * @param isVal boolean value.
         * @param bVal byte value.
         * @param cVal char value.
         * @param sVal short value.
         * @param intVal int value.
         * @param lVal long value.
         * @param fltVal float value.
         * @param dblVal double value.
         * @param strVal String value.
         * @param arrVal Array value.
         * @param val Enum value.
         */
        public EchoJob(boolean isVal, byte bVal, char cVal, short sVal, int intVal, long lVal, float fltVal,
            double dblVal,
            String strVal, Object[] arrVal, TestJobEnum val) {
            this.isVal = isVal;
            this.bVal = bVal;
            this.cVal = cVal;
            this.sVal = sVal;
            this.intVal = intVal;
            this.lVal = lVal;
            this.fltVal = fltVal;
            this.dblVal = dblVal;
            this.strVal = strVal;
            this.arrVal = arrVal;
            eVal = val;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object execute() {
            checkState();

            return argument(0);
        }

        /**
         * Check the object state after serialization / deserialization
         */
        protected void checkState() {
            JobUtils.checkJobState(isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }
    }

    /**
     * Echo job, externalizable. All fields are used only for serialization check.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoJobExternalizable extends EchoJob implements Externalizable {
        /** */
        private boolean isVal;

        /** */
        private byte bVal;

        /** */
        private char cVal;

        /** */
        private short sVal;

        /** */
        private int intVal;

        /** */
        private long lVal;

        /** */
        private float fltVal;

        /** */
        private double dblVal;

        /** */
        private String strVal;

        /** */
        private Object[] arrVal;

        /** */
        private TestJobEnum eVal;

        /**
         * Default constructor (required by {@link Externalizable}).
         */
        public EchoJobExternalizable() {
            // No-op.
        }

        /**
         * @param isVal boolean value.
         * @param bVal byte value.
         * @param cVal char value.
         * @param sVal short value.
         * @param intVal int value.
         * @param lVal long value.
         * @param fltVal float value.
         * @param dblVal double value.
         * @param strVal String value.
         * @param arrVal Array value.
         * @param val Enum value.
         */
        public EchoJobExternalizable(boolean isVal, byte bVal, char cVal, short sVal, int intVal, long lVal,
            float fltVal,
            double dblVal, String strVal, Object[] arrVal, TestJobEnum val) {
            this.isVal = isVal;
            this.bVal = bVal;
            this.cVal = cVal;
            this.sVal = sVal;
            this.intVal = intVal;
            this.lVal = lVal;
            this.fltVal = fltVal;
            this.dblVal = dblVal;
            this.strVal = strVal;
            this.arrVal = arrVal;
            eVal = val;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(argument(0));

            JobUtils.writeJobState(out, isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            setArguments(in.readObject());

            isVal = in.readBoolean();
            bVal = in.readByte();
            cVal = in.readChar();
            sVal = in.readShort();
            intVal = in.readInt();
            lVal = in.readLong();
            fltVal = in.readFloat();
            dblVal = in.readDouble();
            strVal = (String)in.readObject();
            arrVal = (Object[])in.readObject();
            eVal = (TestJobEnum)in.readObject();
        }

        /** {@inheritDoc} */
        @Override protected void checkState() {
            JobUtils.checkJobState(isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }
    }

    /**
     * Echo job, externalizable. All fields are used only for serialization check.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoJobBinarylizable extends EchoJob implements Binarylizable {
        /** */
        private boolean isVal;

        /** */
        private byte bVal;

        /** */
        private char cVal;

        /** */
        private short sVal;

        /** */
        private int intVal;

        /** */
        private long lVal;

        /** */
        private float fltVal;

        /** */
        private double dblVal;

        /** */
        private String strVal;

        /** */
        private Object[] arrVal;

        /** */
        private TestJobEnum eVal;

        /**
         * Default constructor (required by ReflectionFactory).
         */
        public EchoJobBinarylizable() {
            // No-op.
        }

        /**
         * @param isVal boolean value.
         * @param bVal byte value.
         * @param cVal char value.
         * @param sVal short value.
         * @param intVal int value.
         * @param lVal long value.
         * @param fltVal float value.
         * @param dblVal double value.
         * @param strVal String value.
         * @param arrVal Array value.
         * @param val Enum value.
         */
        public EchoJobBinarylizable(boolean isVal, byte bVal, char cVal, short sVal, int intVal, long lVal,
            float fltVal,
            double dblVal, String strVal, Object[] arrVal, TestJobEnum val) {
            this.isVal = isVal;
            this.bVal = bVal;
            this.cVal = cVal;
            this.sVal = sVal;
            this.intVal = intVal;
            this.lVal = lVal;
            this.fltVal = fltVal;
            this.dblVal = dblVal;
            this.strVal = strVal;
            this.arrVal = arrVal;
            eVal = val;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeObject("arg", argument(0));

            JobUtils.writeJobState(writer, isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            Object arg = reader.readObject("arg");

            setArguments(arg);

            isVal = reader.readBoolean("isVal");
            bVal = reader.readByte("bVal");
            cVal = reader.readChar("cVal");
            sVal = reader.readShort("sVal");
            intVal = reader.readInt("intVal");
            lVal = reader.readLong("lVal");
            fltVal = reader.readFloat("fltVal");
            dblVal = reader.readDouble("dblVal");
            strVal = reader.readString("strVal");
            arrVal = reader.readObjectArray("arrVal");
            eVal = reader.readEnum("eVal");
        }

        /** {@inheritDoc} */
        @Override protected void checkState() {
            JobUtils.checkJobState(isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }
    }

    /**
     * Echo job, serializable object. All fields are used only for serialization check.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoClosure implements IgniteClosure<Object, Object> {
        /** */
        private boolean isVal;

        /** */
        private byte bVal;

        /** */
        private char cVal;

        /** */
        private short sVal;

        /** */
        private int intVal;

        /** */
        private long lVal;

        /** */
        private float fltVal;

        /** */
        private double dblVal;

        /** */
        private String strVal;

        /** */
        private Object[] arrVal;

        /** */
        private TestJobEnum eVal;

        /**
         * Default constructor.
         */
        public EchoClosure() {
            // No-op.
        }

        /**
         * @param isVal boolean value.
         * @param bVal byte value.
         * @param cVal char value.
         * @param sVal short value.
         * @param intVal int value.
         * @param lVal long value.
         * @param fltVal float value.
         * @param dblVal double value.
         * @param strVal String value.
         * @param arrVal Array value.
         * @param val Enum value.
         */
        public EchoClosure(boolean isVal, byte bVal, char cVal, short sVal, int intVal, long lVal, float fltVal,
            double dblVal,
            String strVal, Object[] arrVal, TestJobEnum val) {
            this.isVal = isVal;
            this.bVal = bVal;
            this.cVal = cVal;
            this.sVal = sVal;
            this.intVal = intVal;
            this.lVal = lVal;
            this.fltVal = fltVal;
            this.dblVal = dblVal;
            this.strVal = strVal;
            this.arrVal = arrVal;
            eVal = val;
        }

        /** {@inheritDoc} */
        @Override public Object apply(Object arg) {
            checkState();

            return arg;
        }

        /**
         * Check the object state after serialization / deserialization
         */
        protected void checkState() {
            JobUtils.checkJobState(isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }
    }

    /**
     * Echo closure, externalizable. All fields are used only for serialization check.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoClosureExternalizable extends EchoClosure implements Externalizable {
        /** */
        private boolean isVal;

        /** */
        private byte bVal;

        /** */
        private char cVal;

        /** */
        private short sVal;

        /** */
        private int intVal;

        /** */
        private long lVal;

        /** */
        private float fltVal;

        /** */
        private double dblVal;

        /** */
        private String strVal;

        /** */
        private Object[] arrVal;

        /** */
        private TestJobEnum eVal;

        /**
         * Default constructor (required by Externalizable).
         */
        public EchoClosureExternalizable() {
            // No-op
        }

        /**
         * @param isVal boolean value.
         * @param bVal byte value.
         * @param cVal char value.
         * @param sVal short value.
         * @param intVal int value.
         * @param lVal long value.
         * @param fltVal float value.
         * @param dblVal double value.
         * @param strVal String value.
         * @param arrVal Array value.
         * @param val Enum value.
         */
        public EchoClosureExternalizable(boolean isVal, byte bVal, char cVal, short sVal, int intVal, long lVal,
            float fltVal,
            double dblVal, String strVal, Object[] arrVal, TestJobEnum val) {
            this.isVal = isVal;
            this.bVal = bVal;
            this.cVal = cVal;
            this.sVal = sVal;
            this.intVal = intVal;
            this.lVal = lVal;
            this.fltVal = fltVal;
            this.dblVal = dblVal;
            this.strVal = strVal;
            this.arrVal = arrVal;
            eVal = val;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            JobUtils.writeJobState(out, isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            isVal = in.readBoolean();
            bVal = in.readByte();
            cVal = in.readChar();
            sVal = in.readShort();
            intVal = in.readInt();
            lVal = in.readLong();
            fltVal = in.readFloat();
            dblVal = in.readDouble();
            strVal = (String)in.readObject();
            arrVal = (Object[])in.readObject();
            eVal = (TestJobEnum)in.readObject();
        }

        /** {@inheritDoc} */
        @Override protected void checkState() {
            JobUtils.checkJobState(isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }
    }

    /**
     * Echo closure, externalizable. All fields are used only for serialization check.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoClosureBinarylizable extends EchoClosure implements Binarylizable {
        /** */
        private boolean isVal;

        /** */
        private byte bVal;

        /** */
        private char cVal;

        /** */
        private short sVal;

        /** */
        private int intVal;

        /** */
        private long lVal;

        /** */
        private float fltVal;

        /** */
        private double dblVal;

        /** */
        private String strVal;

        /** */
        private Object[] arrVal;

        /** */
        private TestJobEnum eVal;

        /**
         * @param isVal boolean value.
         * @param bVal byte value.
         * @param cVal char value.
         * @param sVal short value.
         * @param intVal int value.
         * @param lVal long value.
         * @param fltVal float value.
         * @param dblVal double value.
         * @param strVal String value.
         * @param arrVal Array value.
         * @param val Enum value.
         */
        public EchoClosureBinarylizable(boolean isVal, byte bVal, char cVal, short sVal, int intVal, long lVal,
            float fltVal,
            double dblVal, String strVal, Object[] arrVal, TestJobEnum val) {
            this.isVal = isVal;
            this.bVal = bVal;
            this.cVal = cVal;
            this.sVal = sVal;
            this.intVal = intVal;
            this.lVal = lVal;
            this.fltVal = fltVal;
            this.dblVal = dblVal;
            this.strVal = strVal;
            this.arrVal = arrVal;
            eVal = val;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            JobUtils.writeJobState(writer, isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            isVal = reader.readBoolean("isVal");
            bVal = reader.readByte("bVal");
            cVal = reader.readChar("cVal");
            sVal = reader.readShort("sVal");
            intVal = reader.readInt("intVal");
            lVal = reader.readLong("lVal");
            fltVal = reader.readFloat("fltVal");
            dblVal = reader.readDouble("dblVal");
            strVal = reader.readString("strVal");
            arrVal = reader.readObjectArray("arrVal");
            eVal = reader.readEnum("eVal");
        }

        /** {@inheritDoc} */
        @Override protected void checkState() {
            JobUtils.checkJobState(isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }
    }

    /**
     * Test callable, serializable object. All fields are used only for serialization check.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoCallable implements IgniteCallable<Object> {
        /** */
        protected Object arg;

        /** */
        private boolean isVal;

        /** */
        private byte bVal;

        /** */
        private char cVal;

        /** */
        private short sVal;

        /** */
        private int intVal;

        /** */
        private long lVal;

        /** */
        private float fltVal;

        /** */
        private double dblVal;

        /** */
        private String strVal;

        /** */
        private Object[] arrVal;

        /** */
        private TestJobEnum eVal;

        /**
         * Default constructor.
         */
        public EchoCallable() {
            // No-op.
        }

        /**
         * @param isVal boolean value.
         * @param bVal byte value.
         * @param cVal char value.
         * @param sVal short value.
         * @param intVal int value.
         * @param lVal long value.
         * @param fltVal float value.
         * @param dblVal double value.
         * @param strVal String value.
         * @param arrVal Array value.
         * @param val Enum value.
         */
        public EchoCallable(boolean isVal, byte bVal, char cVal, short sVal, int intVal, long lVal, float fltVal,
            double dblVal,
            String strVal, Object[] arrVal, TestJobEnum val) {
            this.isVal = isVal;
            this.bVal = bVal;
            this.cVal = cVal;
            this.sVal = sVal;
            this.intVal = intVal;
            this.lVal = lVal;
            this.fltVal = fltVal;
            this.dblVal = dblVal;
            this.strVal = strVal;
            this.arrVal = arrVal;
            eVal = val;
        }

        /**
         * @param arg Argument.
         */
        void setArg(@Nullable Object arg) {
            this.arg = arg;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object call() throws Exception {
            checkState();

            return arg;
        }

        /**
         * Check the object state after serialization / deserialization
         */
        protected void checkState() {
            JobUtils.checkJobState(isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }
    }

    /**
     * Echo callable, externalizable object. All fields are used only for serialization check.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoCallableExternalizable extends EchoCallable implements Externalizable {
        /** */
        private boolean isVal;

        /** */
        private byte bVal;

        /** */
        private char cVal;

        /** */
        private short sVal;

        /** */
        private int intVal;

        /** */
        private long lVal;

        /** */
        private float fltVal;

        /** */
        private double dblVal;

        /** */
        private String strVal;

        /** */
        private Object[] arrVal;

        /** */
        private TestJobEnum eVal;

        /**
         * Default constructor.
         */
        public EchoCallableExternalizable() {
            // No-op.
        }

        /**
         * @param isVal boolean value.
         * @param bVal byte value.
         * @param cVal char value.
         * @param sVal short value.
         * @param intVal int value.
         * @param lVal long value.
         * @param fltVal float value.
         * @param dblVal double value.
         * @param strVal String value.
         * @param arrVal Array value.
         * @param val Enum value.
         */
        public EchoCallableExternalizable(boolean isVal, byte bVal, char cVal, short sVal, int intVal, long lVal,
            float fltVal,
            double dblVal, String strVal, Object[] arrVal, TestJobEnum val) {
            this.isVal = isVal;
            this.bVal = bVal;
            this.cVal = cVal;
            this.sVal = sVal;
            this.intVal = intVal;
            this.lVal = lVal;
            this.fltVal = fltVal;
            this.dblVal = dblVal;
            this.strVal = strVal;
            this.arrVal = arrVal;
            eVal = val;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(arg);

            JobUtils.writeJobState(out, isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            arg = in.readObject();

            isVal = in.readBoolean();
            bVal = in.readByte();
            cVal = in.readChar();
            sVal = in.readShort();
            intVal = in.readInt();
            lVal = in.readLong();
            fltVal = in.readFloat();
            dblVal = in.readDouble();
            strVal = (String)in.readObject();
            arrVal = (Object[])in.readObject();
            eVal = (TestJobEnum)in.readObject();
        }

        /** {@inheritDoc} */
        @Override protected void checkState() {
            JobUtils.checkJobState(isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }
    }

    /**
     * Echo callable, binarylizable object. All fields are used only for serialization check.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class EchoCallableBinarylizable extends EchoCallable implements Binarylizable {
        /** */
        private boolean isVal;

        /** */
        private byte bVal;

        /** */
        private char cVal;

        /** */
        private short sVal;

        /** */
        private int intVal;

        /** */
        private long lVal;

        /** */
        private float fltVal;

        /** */
        private double dblVal;

        /** */
        private String strVal;

        /** */
        private Object[] arrVal;

        /** */
        private TestJobEnum eVal;

        /**
         * Default constructor.
         */
        public EchoCallableBinarylizable() {
            // No-op.
        }

        /**
         * @param isVal boolean value.
         * @param bVal byte value.
         * @param cVal char value.
         * @param sVal short value.
         * @param intVal int value.
         * @param lVal long value.
         * @param fltVal float value.
         * @param dblVal double value.
         * @param strVal String value.
         * @param arrVal Array value.
         * @param val Enum value.
         */
        public EchoCallableBinarylizable(boolean isVal, byte bVal, char cVal, short sVal, int intVal, long lVal,
            float fltVal,
            double dblVal, String strVal, Object[] arrVal, TestJobEnum val) {
            this.isVal = isVal;
            this.bVal = bVal;
            this.cVal = cVal;
            this.sVal = sVal;
            this.intVal = intVal;
            this.lVal = lVal;
            this.fltVal = fltVal;
            this.dblVal = dblVal;
            this.strVal = strVal;
            this.arrVal = arrVal;
            eVal = val;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeObject("arg", arg);

            JobUtils.writeJobState(writer, isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            arg = reader.readObject("arg");

            isVal = reader.readBoolean("isVal");
            bVal = reader.readByte("bVal");
            cVal = reader.readChar("cVal");
            sVal = reader.readShort("sVal");
            intVal = reader.readInt("intVal");
            lVal = reader.readLong("lVal");
            fltVal = reader.readFloat("fltVal");
            dblVal = reader.readDouble("dblVal");
            strVal = reader.readString("strVal");
            arrVal = reader.readObjectArray("arrVal");
            eVal = reader.readEnum("eVal");
        }

        /** {@inheritDoc} */
        @Override protected void checkState() {
            JobUtils.checkJobState(isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }
    }

    /**
     * Test runnable, serializable object. All fields are used only for serialization check.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class ComputeTestRunnable implements IgniteRunnable {
        /** */
        private boolean isVal;

        /** */
        private byte bVal;

        /** */
        private char cVal;

        /** */
        private short sVal;

        /** */
        private int intVal;

        /** */
        private long lVal;

        /** */
        private float fltVal;

        /** */
        private double dblVal;

        /** */
        private String strVal;

        /** */
        private Object[] arrVal;

        /** */
        private TestJobEnum eVal;

        /**
         * Default constructor.
         */
        public ComputeTestRunnable() {
            // No-op.
        }

        /**
         * @param isVal boolean value.
         * @param bVal byte value.
         * @param cVal char value.
         * @param sVal short value.
         * @param intVal int value.
         * @param lVal long value.
         * @param fltVal float value.
         * @param dblVal double value.
         * @param strVal String value.
         * @param arrVal Array value.
         * @param val Enum value.
         */
        public ComputeTestRunnable(boolean isVal, byte bVal, char cVal, short sVal, int intVal, long lVal, float fltVal,
            double dblVal, String strVal, Object[] arrVal, TestJobEnum val) {
            this.isVal = isVal;
            this.bVal = bVal;
            this.cVal = cVal;
            this.sVal = sVal;
            this.intVal = intVal;
            this.lVal = lVal;
            this.fltVal = fltVal;
            this.dblVal = dblVal;
            this.strVal = strVal;
            this.arrVal = arrVal;
            eVal = val;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            checkState();
        }

        /**
         * Check the object state after serialization / deserialization
         */
        protected void checkState() {
            JobUtils.checkJobState(isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }
    }

    /**
     * Test runnable, externalizable object. All fields are used only for serialization check.
     */
    public static class ComputeTestRunnableExternalizable extends ComputeTestRunnable implements Externalizable {
        /** */
        private boolean isVal;

        /** */
        private byte bVal;

        /** */
        private char cVal;

        /** */
        private short sVal;

        /** */
        private int intVal;

        /** */
        private long lVal;

        /** */
        private float fltVal;

        /** */
        private double dblVal;

        /** */
        private String strVal;

        /** */
        private Object[] arrVal;

        /** */
        private TestJobEnum eVal;

        /**
         * Default constructor (required by Externalizable).
         */
        public ComputeTestRunnableExternalizable() {
            // No-op
        }

        /**
         * @param isVal boolean value.
         * @param bVal byte value.
         * @param cVal char value.
         * @param sVal short value.
         * @param intVal int value.
         * @param lVal long value.
         * @param fltVal float value.
         * @param dblVal double value.
         * @param strVal String value.
         * @param arrVal Array value.
         * @param val Enum value.
         */
        public ComputeTestRunnableExternalizable(boolean isVal, byte bVal, char cVal, short sVal, int intVal, long lVal,
            float fltVal, double dblVal, String strVal, Object[] arrVal,
            TestJobEnum val) {
            this.intVal = intVal;
            this.isVal = isVal;
            this.bVal = bVal;
            this.cVal = cVal;
            this.sVal = sVal;
            this.lVal = lVal;
            this.fltVal = fltVal;
            this.dblVal = dblVal;
            this.strVal = strVal;
            this.arrVal = arrVal;
            eVal = val;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            JobUtils.writeJobState(out, isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            isVal = in.readBoolean();
            bVal = in.readByte();
            cVal = in.readChar();
            sVal = in.readShort();
            intVal = in.readInt();
            lVal = in.readLong();
            fltVal = in.readFloat();
            dblVal = in.readDouble();
            strVal = (String)in.readObject();
            arrVal = (Object[])in.readObject();
            eVal = (TestJobEnum)in.readObject();
        }

        /** {@inheritDoc} */
        @Override protected void checkState() {
            JobUtils.checkJobState(isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }
    }

    /**
     * Test runnable, binarylizable object. All fields are used only for serialization check.
     */
    public static class ComputeTestRunnableBinarylizable extends ComputeTestRunnable implements Binarylizable {
        /** */
        private boolean isVal;

        /** */
        private byte bVal;

        /** */
        private char cVal;

        /** */
        private short sVal;

        /** */
        private int intVal;

        /** */
        private long lVal;

        /** */
        private float fltVal;

        /** */
        private double dblVal;

        /** */
        private String strVal;

        /** */
        private Object[] arrVal;

        /** */
        private TestJobEnum eVal;

        /**
         * Default constructor.
         */
        public ComputeTestRunnableBinarylizable() {
            // No-op.
        }

        /**
         * @param isVal boolean value.
         * @param bVal byte value.
         * @param cVal char value.
         * @param sVal short value.
         * @param intVal int value.
         * @param lVal long value.
         * @param fltVal float value.
         * @param dblVal double value.
         * @param strVal String value.
         * @param arrVal Array value.
         * @param val Enum value.
         */
        public ComputeTestRunnableBinarylizable(boolean isVal, byte bVal, char cVal, short sVal, int intVal, long lVal,
            float fltVal, double dblVal, String strVal, Object[] arrVal,
            TestJobEnum val) {
            this.isVal = isVal;
            this.bVal = bVal;
            this.cVal = cVal;
            this.sVal = sVal;
            this.intVal = intVal;
            this.lVal = lVal;
            this.fltVal = fltVal;
            this.dblVal = dblVal;
            this.strVal = strVal;
            this.arrVal = arrVal;
            eVal = val;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            JobUtils.writeJobState(writer, isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            isVal = reader.readBoolean("isVal");
            bVal = reader.readByte("bVal");
            cVal = reader.readChar("cVal");
            sVal = reader.readShort("sVal");
            intVal = reader.readInt("intVal");
            lVal = reader.readLong("lVal");
            fltVal = reader.readFloat("fltVal");
            dblVal = reader.readDouble("dblVal");
            strVal = reader.readString("strVal");
            arrVal = reader.readObjectArray("arrVal");
            eVal = reader.readEnum("eVal");
        }

        /** {@inheritDoc} */
        @Override protected void checkState() {
            JobUtils.checkJobState(isVal, bVal, cVal, sVal, intVal, lVal, fltVal, dblVal, strVal, arrVal, eVal);
        }
    }

    /**
     * Creates test jobs with tested parameters
     */
    private static class JobFactory<T> implements Factory<T> {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        private Class<?> cls;

        /**
         * @param cls Class.
         */
        JobFactory(Class<?> cls) {
            this.cls = cls;
        }

        /** {@inheritDoc} */
        @Override public T create() {
            try {
                Constructor<?> constructor = cls.getConstructor(Boolean.TYPE, Byte.TYPE, Character.TYPE,
                    Short.TYPE, Integer.TYPE, Long.TYPE, Float.TYPE, Double.TYPE, String.class, Object[].class, TestJobEnum.class);

                return (T)constructor.newInstance(true,
                    Byte.MAX_VALUE,
                    Character.MAX_VALUE,
                    Short.MAX_VALUE,
                    Integer.MAX_VALUE,
                    Long.MAX_VALUE,
                    Float.MAX_VALUE,
                    Double.MAX_VALUE,
                    STR_VAL, ARRAY_VAL,
                    TestJobEnum.VALUE_2);
            }
            catch (NoSuchMethodException | InstantiationException | InvocationTargetException |
                IllegalAccessException e) {
                throw new IgniteException("Failed to create object using default constructor: " + cls, e);
            }
        }
    }

    /**
     * Collection of utility methods to simplify EchoJob*, EchoCLosure*, EchoCallable* and ComputeTestRunnable* classes
     */
    private static class JobUtils {
        /**
         * @param isVal boolean value.
         * @param bVal byte value.
         * @param cVal char value.
         * @param sVal short value.
         * @param intVal int value.
         * @param lVal long value.
         * @param fltVal float value.
         * @param dblVal double value.
         * @param strVal String value.
         * @param arrVal Array value.
         * @param eVal Enum value.
         */
        private static void checkJobState(boolean isVal, byte bVal, char cVal, short sVal, int intVal, long lVal,
            float fltVal,
            double dblVal, String strVal, Object[] arrVal, TestJobEnum eVal) {
            assertEquals(true, isVal);
            assertEquals(Byte.MAX_VALUE, bVal);
            assertEquals(Character.MAX_VALUE, cVal);
            assertEquals(Short.MAX_VALUE, sVal);
            assertEquals(Integer.MAX_VALUE, intVal);
            assertEquals(Long.MAX_VALUE, lVal);
            assertEquals(Float.MAX_VALUE, fltVal);
            assertEquals(Double.MAX_VALUE, dblVal);
            assertEquals(STR_VAL, strVal);
            Assert.assertArrayEquals(ARRAY_VAL, arrVal);
            assertEquals(TestJobEnum.VALUE_2, eVal);
        }

        /**
         * @param writer Writer.
         * @param isVal boolean value.
         * @param bVal byte value.
         * @param cVal char value.
         * @param sVal short value.
         * @param intVal int value.
         * @param lVal long value.
         * @param fltVal float value.
         * @param dblVal double value.
         * @param strVal String value.
         * @param arrVal Array value.
         * @param eVal Enum value.
         * @throws BinaryObjectException If failed.
         */
        private static void writeJobState(BinaryWriter writer, boolean isVal, byte bVal, char cVal, short sVal,
            int intVal, long lVal, float fltVal, double dblVal, String strVal,
            Object[] arrVal, TestJobEnum eVal) throws BinaryObjectException {
            writer.writeBoolean("isVal", isVal);
            writer.writeByte("bVal", bVal);
            writer.writeChar("cVal", cVal);
            writer.writeShort("sVal", sVal);
            writer.writeInt("intVal", intVal);
            writer.writeLong("lVal", lVal);
            writer.writeFloat("fltVal", fltVal);
            writer.writeDouble("dblVal", dblVal);
            writer.writeString("strVal", strVal);
            writer.writeObjectArray("arrVal", arrVal);
            writer.writeEnum("eVal", eVal);
        }

        /**
         * @param out Out.
         * @param isVal boolean value.
         * @param bVal byte value.
         * @param cVal char value.
         * @param sVal short value.
         * @param intVal int value.
         * @param lVal long value.
         * @param fltVal float value.
         * @param dblVal double value.
         * @param strVal String value.
         * @param arrVal Array value.
         * @param eVal Enum value.
         * @throws IOException If failed.
         */
        private static void writeJobState(ObjectOutput out, boolean isVal, byte bVal, char cVal, short sVal,
            int intVal, long lVal, float fltVal, double dblVal, String strVal, Object[] arrVal,
            TestJobEnum eVal) throws IOException {
            out.writeBoolean(isVal);
            out.writeByte(bVal);
            out.writeChar(cVal);
            out.writeShort(sVal);
            out.writeInt(intVal);
            out.writeLong(lVal);
            out.writeFloat(fltVal);
            out.writeDouble(dblVal);
            out.writeObject(strVal);
            out.writeObject(arrVal);
            out.writeObject(eVal);
        }
    }
}