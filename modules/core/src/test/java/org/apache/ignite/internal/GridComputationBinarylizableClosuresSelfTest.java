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
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class GridComputationBinarylizableClosuresSelfTest extends GridCommonAbstractTest {

    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        TestBinarylizableJob.writeCalled.set(false);
        TestBinarylizableJob.readCalled.set(false);
        TestBinarylizableJob.executed.set(false);

        TestBinarylizableCallable.writeCalled.set(false);
        TestBinarylizableCallable.readCalled.set(false);
        TestBinarylizableCallable.executed.set(false);

        TestBinarylizableRunnable.writeCalled.set(false);
        TestBinarylizableRunnable.readCalled.set(false);
        TestBinarylizableRunnable.executed.set(false);
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    public void testJob() throws Exception {
        Ignite ignite = startGrid(1);
        startGrid(2);

        final TestBinarylizableJob job = new TestBinarylizableJob();

        ignite.compute(ignite.cluster().forRemotes()).apply(job, (Object)null);

        assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return TestBinarylizableJob.writeCalled.get() && TestBinarylizableJob.readCalled.get() &&
                    TestBinarylizableJob.executed.get();
            }
        }, 5000);
    }

    public void testCallable() throws Exception {
        Ignite ignite = startGrid(1);
        startGrid(2);

        final TestBinarylizableCallable callable = new TestBinarylizableCallable();

        ignite.compute(ignite.cluster().forRemotes()).call(callable);

        assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return TestBinarylizableCallable.writeCalled.get() && TestBinarylizableCallable.readCalled.get() &&
                    TestBinarylizableCallable.executed.get();
            }
        }, 5000);
    }

    public void testRunnable() throws Exception {
        Ignite ignite = startGrid(1);
        startGrid(2);

        final TestBinarylizableRunnable runnable = new TestBinarylizableRunnable();

        ignite.compute(ignite.cluster().forRemotes()).run(runnable);

        assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return TestBinarylizableRunnable.writeCalled.get() && TestBinarylizableRunnable.readCalled.get() &&
                    TestBinarylizableRunnable.executed.get();
            }
        }, 5000);
    }

    private static class TestBinarylizableJob implements IgniteClosure, Binarylizable {

        private static AtomicBoolean writeCalled = new AtomicBoolean();
        private static AtomicBoolean readCalled = new AtomicBoolean();
        private static AtomicBoolean executed = new AtomicBoolean();

        @Override public Object apply(Object o) {
            executed.set(true);
            return null;
        }

        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writeCalled.set(true);
        }

        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            readCalled.set(true);
        }
    }

    private static class TestBinarylizableCallable implements IgniteCallable, Binarylizable {

        private static AtomicBoolean writeCalled = new AtomicBoolean();
        private static AtomicBoolean readCalled = new AtomicBoolean();
        private static AtomicBoolean executed = new AtomicBoolean();

        @Override public Object call() throws Exception {
            executed.set(true);
            return null;
        }

        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writeCalled.set(true);
        }

        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            readCalled.set(true);
        }
    }

    private static class TestBinarylizableRunnable implements IgniteRunnable, Binarylizable {

        private static AtomicBoolean writeCalled = new AtomicBoolean();
        private static AtomicBoolean readCalled = new AtomicBoolean();
        private static AtomicBoolean executed = new AtomicBoolean();

        @Override public void run() {
            executed.set(true);
        }

        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writeCalled.set(true);
        }

        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            readCalled.set(true);
        }
    }

}
