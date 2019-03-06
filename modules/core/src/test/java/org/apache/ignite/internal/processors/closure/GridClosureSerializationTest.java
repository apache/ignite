/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.junit.Test;

/**
 * Tests handling of job result serialization error.
 */
public class GridClosureSerializationTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
        startGrid(1);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"Convert2Lambda"})
    @Test
    public void testSerializationFailure() throws Exception {
        final IgniteEx ignite0 = grid(0);
        final IgniteEx ignite1 = grid(1);

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
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
    @SuppressWarnings({"Convert2Lambda"})
    @Test
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
    @Test
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
