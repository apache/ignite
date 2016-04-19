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
import java.util.function.*;
import java.util.stream.*;

/**
 * Full API compute test.
 */
public class IgniteComputeConfigVariationsFullApiTest extends IgniteConfigVariationsAbstractTest {
    /** Max job count. */
    private static final int MAX_JOB_COUNT = 10;

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

    /**
     * @param test test object, job factory is passed as a parameter.
     */
    private void runWithAllFactories(Consumer<Factory<ComputeJobAdapter>> test, Factory[] factories) throws Exception {
        for (int i = 0; i < factories.length; i++) {
            Factory factory = factories[i];

            info("Running test with jobs model: " + factory.create().getClass().getName());

            if (i != 0)
                beforeTest();

            try {
                test.accept((Factory<ComputeJobAdapter>)factory);
            }
            finally {
                if (i + 1 != factories.length)
                    afterTest();
            }
        }
    }

    /**
     * The wrapper for tests. Provides variations of the argument data model and job object data model
     *
     * @param test Test.
     */
    private void runTest(Factory[] factories, Consumer<Factory<ComputeJobAdapter>> test) throws Exception {
        runInAllDataModes(() -> {
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
        });
    }

    /**
     */
    public void testExecuteTaskClass() throws Exception {
        runTest(jobFactories, (factory) -> {
            final int[] i = {-1};

            List<Object> results = grid(0).compute().execute(TestTask.class,
                new T2<>(factory, () -> value(i[0]++)));

            checkExecuteJobResults(results);
        });
    }

    /**
     */
    public void testExecuteTask() throws Exception {
        runTest(jobFactories, (factory) -> {
            final int[] i = {-1};

            List<Object> results = grid(0).compute().execute(new TestTask(),
                new T2<>(factory, () -> value(i[0]++)));

            checkExecuteJobResults(results);
        });
    }

    /**
     */
    public void testBroadcast() throws Exception {
        runTest(closureFactories, (factory) -> {

            IgniteCompute comp = grid(0).compute();
            Collection<Object> results = comp.broadcast((IgniteClosure<Object, Object>)factory.create(), value(-1));

            assertEquals(gridCount(), results.size());
            assertEquals(gridCount(), results.stream().filter(o -> o == null).count());

            results = grid(0).compute()
                .broadcast((IgniteClosure<Object, Object>)factory.create(), value(0));

            final Class dataCls = value(0).getClass();
            assertEquals(gridCount(), results.stream().filter(o -> o != null)
                .filter(o -> dataCls.equals(o.getClass())).count());
        });
    }

    /**
     */
    public void testApply() throws Exception {
        runTest(closureFactories, (factory) -> {
            IgniteCompute comp = grid(0).compute().withAsync();

            List<ComputeTaskFuture<Object>> futures = IntStream.range(-1, MAX_JOB_COUNT - 1).mapToObj(i -> {
                comp.apply((IgniteClosure<Object, Object>)factory.create(), value(i));
                return comp.future();
            }).collect(Collectors.toList());

            Collection<Object> results = futures.stream().map(ComputeTaskFuture::get)
                .collect(Collectors.toList());

            checkExecuteJobResults(results);
        });
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object value(int valId) {
        if (valId < 0)
            return null;
        return super.value(valId);
    }

    /**
     * @param results Results.
     */
    private void checkExecuteJobResults(Collection<Object> results) {
        final Class dataCls = value(0).getClass();
        assertEquals("Result objects type mismatch (null values are filtered)",
            MAX_JOB_COUNT - 1, // null value is filtered
            results.stream().filter(o -> o != null)
                .filter(o -> dataCls.equals(o.getClass())).count());
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
            return results.stream().map(ComputeJobResult::getData).collect(Collectors.toList());
        }
    }

    /**
     * Echo job, plain object
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
     * Echo job, plain object
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

        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {

        }

        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {

        }
    }
}
