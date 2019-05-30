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
 *
 */

package org.apache.ignite.internal;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.compute.ComputeJobMasterLeaveAware;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test ensuring that correct closures are serialized.
 */
public class GridComputationBinarylizableClosuresSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        TestBinarylizableClosure.writeCalled.set(false);
        TestBinarylizableClosure.readCalled.set(false);
        TestBinarylizableClosure.executed.set(false);

        TestBinarylizableMasterLeaveAwareClosure.writeCalled.set(false);
        TestBinarylizableMasterLeaveAwareClosure.readCalled.set(false);

        TestBinarylizableCallable.writeCalled.set(false);
        TestBinarylizableCallable.readCalled.set(false);
        TestBinarylizableCallable.executed.set(false);

        TestBinarylizableMasterLeaveAwareCallable.writeCalled.set(false);
        TestBinarylizableMasterLeaveAwareCallable.readCalled.set(false);

        TestBinarylizableRunnable.writeCalled.set(false);
        TestBinarylizableRunnable.readCalled.set(false);
        TestBinarylizableRunnable.executed.set(false);

        TestBinarylizableMasterLeaveAwareRunnable.writeCalled.set(false);
        TestBinarylizableMasterLeaveAwareRunnable.readCalled.set(false);

        TestBinarylizableObject.writeCalled.set(false);
        TestBinarylizableObject.readCalled.set(false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Test that Binarylizable IgniteClosure is serialized using BinaryMarshaller.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testJob() throws Exception {
        Ignite ignite = startGrid(1);
        startGrid(2);

        final TestBinarylizableClosure closure = new TestBinarylizableClosure();

        ignite.compute(ignite.cluster().forRemotes()).apply(closure, new TestBinarylizableObject());

        assert TestBinarylizableClosure.executed.get();
        assert TestBinarylizableClosure.writeCalled.get();
        assert TestBinarylizableClosure.readCalled.get();

        assert TestBinarylizableObject.writeCalled.get();
        assert TestBinarylizableObject.readCalled.get();
    }

    /**
     * Test that Binarylizable IgniteClosure with ComputeJobMasterLeaveAware interface is serialized
     * using BinaryMarshaller.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMasterLeaveAwareJob() throws Exception {
        Ignite ignite = startGrid(1);
        startGrid(2);

        final TestBinarylizableMasterLeaveAwareClosure job = new TestBinarylizableMasterLeaveAwareClosure();

        ignite.compute(ignite.cluster().forRemotes()).apply(job, new TestBinarylizableObject());

        assert TestBinarylizableClosure.executed.get();
        assert TestBinarylizableClosure.writeCalled.get();
        assert TestBinarylizableClosure.readCalled.get();

        assert TestBinarylizableMasterLeaveAwareClosure.writeCalled.get();
        assert TestBinarylizableMasterLeaveAwareClosure.readCalled.get();

        assert TestBinarylizableObject.writeCalled.get();
        assert TestBinarylizableObject.readCalled.get();
    }

    /**
     * Test that Binarylizable IgniteCallable is serialized using BinaryMarshaller.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCallable() throws Exception {
        Ignite ignite = startGrid(1);
        startGrid(2);

        final TestBinarylizableCallable callable = new TestBinarylizableCallable();

        ignite.compute(ignite.cluster().forRemotes()).call(callable);

        assert TestBinarylizableCallable.executed.get();
        assert TestBinarylizableCallable.writeCalled.get();
        assert TestBinarylizableCallable.readCalled.get();
    }

    /**
     * Test that Binarylizable IgniteCallable with ComputeJobMasterLeaveAware interface is serialized
     * using BinaryMarshaller.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMasterLeaveAwareCallable() throws Exception {
        Ignite ignite = startGrid(1);
        startGrid(2);

        final TestBinarylizableMasterLeaveAwareCallable callable = new TestBinarylizableMasterLeaveAwareCallable();

        ignite.compute(ignite.cluster().forRemotes()).call(callable);

        assert TestBinarylizableCallable.executed.get();
        assert TestBinarylizableCallable.writeCalled.get();
        assert TestBinarylizableCallable.readCalled.get();

        assert TestBinarylizableMasterLeaveAwareCallable.writeCalled.get();
        assert TestBinarylizableMasterLeaveAwareCallable.readCalled.get();
    }

    /**
     * Test that Binarylizable IgniteRunnable is serialized using BinaryMarshaller.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRunnable() throws Exception {
        Ignite ignite = startGrid(1);
        startGrid(2);

        final TestBinarylizableRunnable runnable = new TestBinarylizableRunnable();

        ignite.compute(ignite.cluster().forRemotes()).run(runnable);

        assert TestBinarylizableRunnable.executed.get();
        assert TestBinarylizableRunnable.writeCalled.get();
        assert TestBinarylizableRunnable.readCalled.get();
    }

    /**
     * Test that Binarylizable IgniteRunnable with ComputeJobMasterLeaveAware interface is serialized
     * using BinaryMarshaller.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMasterLeaveAwareRunnable() throws Exception {
        Ignite ignite = startGrid(1);
        startGrid(2);

        final TestBinarylizableMasterLeaveAwareRunnable runnable = new TestBinarylizableMasterLeaveAwareRunnable();

        ignite.compute(ignite.cluster().forRemotes()).run(runnable);

        assert TestBinarylizableRunnable.executed.get();
        assert TestBinarylizableRunnable.writeCalled.get();
        assert TestBinarylizableRunnable.readCalled.get();

        assert TestBinarylizableMasterLeaveAwareRunnable.writeCalled.get();
        assert TestBinarylizableMasterLeaveAwareRunnable.readCalled.get();
    }

    /**
     * Test Binarylizable IgniteClosure.
     */
    private static class TestBinarylizableClosure implements IgniteClosure, Binarylizable {

        /** Tracks {@link TestBinarylizableClosure::writeBinary(BinaryWriter writer)} calls. */
        private static AtomicBoolean writeCalled = new AtomicBoolean();

        /** Tracks {@link TestBinarylizableClosure::readBinary(BinaryReader reader)} calls. */
        private static AtomicBoolean readCalled = new AtomicBoolean();

        /** Tracks {@link TestBinarylizableClosure::apply(Object o)} calls. */
        private static AtomicBoolean executed = new AtomicBoolean();

        /** {@inheritDoc} */
        @Override public Object apply(Object o) {
            executed.set(true);
            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writeCalled.set(true);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            readCalled.set(true);
        }
    }

    /**
     * Test Binarylizable IgniteClosure with ComputeJobMasterLeaveAware interface.
     */
    private static class TestBinarylizableMasterLeaveAwareClosure extends TestBinarylizableClosure
        implements ComputeJobMasterLeaveAware {

        /** Tracks {@link TestBinarylizableMasterLeaveAwareClosure::writeBinary(BinaryWriter writer)} calls. */
        private static AtomicBoolean writeCalled = new AtomicBoolean();

        /** Tracks {@link TestBinarylizableMasterLeaveAwareClosure::readBinary(BinaryReader reader)} calls. */
        private static AtomicBoolean readCalled = new AtomicBoolean();

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            super.writeBinary(writer);
            writeCalled.set(true);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            super.readBinary(reader);
            readCalled.set(true);
        }

        /** {@inheritDoc} */
        @Override public void onMasterNodeLeft(ComputeTaskSession ses) throws IgniteException {
        }
    }

    /**
     * Test Binarylizable object.
     */
    private static class TestBinarylizableObject implements Binarylizable {

        /** Tracks {@link TestBinarylizableObject::writeBinary(BinaryWriter writer)} calls. */
        private static AtomicBoolean writeCalled = new AtomicBoolean();

        /** Tracks {@link TestBinarylizableObject::readBinary(BinaryReader reader)} calls. */
        private static AtomicBoolean readCalled = new AtomicBoolean();

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writeCalled.set(true);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            readCalled.set(true);
        }
    }

    /**
     * Test Binarylizable Callable.
     */
    private static class TestBinarylizableCallable implements IgniteCallable, Binarylizable {

        /** Tracks {@link TestBinarylizableCallable::writeBinary(BinaryWriter writer)} calls. */
        private static AtomicBoolean writeCalled = new AtomicBoolean();

        /** Tracks {@link TestBinarylizableCallable::readBinary(BinaryReader reader)} calls. */
        private static AtomicBoolean readCalled = new AtomicBoolean();

        /** Tracks {@link TestBinarylizableCallable::call()} calls. */
        private static AtomicBoolean executed = new AtomicBoolean();

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            executed.set(true);
            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writeCalled.set(true);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            readCalled.set(true);
        }
    }

    /**
     * Test Binarylizable Callable with ComputeJobMasterLeaveAware interface.
     */
    private static class TestBinarylizableMasterLeaveAwareCallable extends TestBinarylizableCallable
        implements ComputeJobMasterLeaveAware {

        /** Tracks {@link TestBinarylizableMasterLeaveAwareCallable::writeBinary(BinaryWriter writer)} calls. */
        private static AtomicBoolean writeCalled = new AtomicBoolean();

        /** Tracks {@link TestBinarylizableMasterLeaveAwareCallable::readBinary(BinaryReader reader)} calls. */
        private static AtomicBoolean readCalled = new AtomicBoolean();

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            super.writeBinary(writer);
            writeCalled.set(true);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            super.readBinary(reader);
            readCalled.set(true);
        }

        /** {@inheritDoc} */
        @Override public void onMasterNodeLeft(ComputeTaskSession ses) throws IgniteException {
        }
    }

    /**
     * Test Binarylizable Runnable.
     */
    private static class TestBinarylizableRunnable implements IgniteRunnable, Binarylizable {

        /** Tracks {@link TestBinarylizableRunnable::writeBinary(BinaryWriter writer)} calls. */
        private static AtomicBoolean writeCalled = new AtomicBoolean();

        /** Tracks {@link TestBinarylizableRunnable::readBinary(BinaryReader reader)} calls. */
        private static AtomicBoolean readCalled = new AtomicBoolean();

        /** Tracks {@link TestBinarylizableRunnable::run()} calls. */
        private static AtomicBoolean executed = new AtomicBoolean();

        /** {@inheritDoc} */
        @Override public void run() {
            executed.set(true);
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writeCalled.set(true);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            readCalled.set(true);
        }
    }

    /**
     * Test Binarylizable Runnable with ComputeJobMasterLeaveAware interface.
     */
    private static class TestBinarylizableMasterLeaveAwareRunnable extends TestBinarylizableRunnable
        implements ComputeJobMasterLeaveAware {

        /** Tracks {@link TestBinarylizableMasterLeaveAwareRunnable::writeBinary(BinaryWriter writer)} calls. */
        private static AtomicBoolean writeCalled = new AtomicBoolean();

        /** Tracks {@link TestBinarylizableMasterLeaveAwareRunnable::readBinary(BinaryReader reader)} calls. */
        private static AtomicBoolean readCalled = new AtomicBoolean();

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            super.writeBinary(writer);
            writeCalled.set(true);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            super.readBinary(reader);
            readCalled.set(true);
        }

        /** {@inheritDoc} */
        @Override public void onMasterNodeLeft(ComputeTaskSession ses) throws IgniteException {
        }
    }

}
