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

package org.apache.ignite.internal.processors.closure;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests handling of job result serialization error.
 */
public class GridClosureSerializationTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
        startGrid(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "Convert2Lambda"})
    public void testSerializationFailure() throws Exception {
        final IgniteEx ignite0 = grid(0);
        final IgniteEx ignite1 = grid(1);

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite1.compute(ignite1.cluster().forNode(ignite0.localNode())).call(new IgniteCallable<Object>() {
                    @Override public Object call() throws Exception {
                        return new CaseClass.CaseClass2();
                    }
                });

                return null;
            }
        }, BinaryObjectException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "Convert2Lambda"})
    public void testExceptionSerializationFailure() throws Exception {
        final IgniteEx ignite0 = grid(0);
        final IgniteEx ignite1 = grid(1);

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite1.compute(ignite1.cluster().forNode(ignite0.localNode())).call(new IgniteCallable<Object>() {
                    @Override public Object call() throws Exception {
                        throw new BrokenException();
                    }
                });

                return null;
            }
        }, IgniteException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "Convert2Lambda"})
    public void testAttributesSerializationFailure() throws Exception {
        final IgniteEx ignite0 = grid(0);
        final IgniteEx ignite1 = grid(1);

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @JobContextResource
            private ComputeJobContext jobCtx;

            @Override public Object call() throws Exception {
                ignite1.compute(ignite1.cluster().forNode(ignite0.localNode())).call(new IgniteCallable<Object>() {
                    @Override public Object call() throws Exception {
                        jobCtx.setAttribute("test-attr", new BrokenAttribute());

                        return null;
                    }
                });

                return null;
            }
        }, IgniteException.class, null);
    }

    /**
     * Binary marshaller will fail because subclass defines other field with different case.
     */
    @SuppressWarnings("unused")
    private static class CaseClass {
        /** */
        private String val;

        /**
         *
         */
        private static class CaseClass2 extends CaseClass {
            /** */
            private String vAl;
        }
    }

    /**
     *
     */
    private static class BrokenAttribute implements Externalizable {
        /** {@inheritDoc} */
        @Override public void writeExternal(final ObjectOutput out) throws IOException {
            throw new IOException("Test exception");
        }

        /** {@inheritDoc} */
        @Override public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
            throw new IOException("Test exception");
        }
    }

    /**
     *
     */
    private static class BrokenException extends Exception implements Externalizable {
        /** {@inheritDoc} */
        @Override public void writeExternal(final ObjectOutput out) throws IOException {
            throw new IOException("Test exception");
        }

        /** {@inheritDoc} */
        @Override public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
            throw new IOException("Test exception");
        }
    }
}
