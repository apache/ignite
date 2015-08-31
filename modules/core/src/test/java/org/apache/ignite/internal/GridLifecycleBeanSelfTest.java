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

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteState;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static org.apache.ignite.lifecycle.LifecycleEventType.AFTER_NODE_START;
import static org.apache.ignite.lifecycle.LifecycleEventType.AFTER_NODE_STOP;
import static org.apache.ignite.lifecycle.LifecycleEventType.BEFORE_NODE_START;
import static org.apache.ignite.lifecycle.LifecycleEventType.BEFORE_NODE_STOP;

/**
 * Lifecycle bean test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridLifecycleBeanSelfTest extends GridCommonAbstractTest {
    /** */
    private LifeCycleBaseBean bean;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setLifecycleBeans(bean);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetIgnite() throws Exception {
        final AtomicBoolean done = new AtomicBoolean();

        bean = new LifeCycleBaseBean() {
            /** */
            @IgniteInstanceResource
            private Ignite ignite;

            @Override public void onLifecycleEvent(LifecycleEventType evt) {
                super.onLifecycleEvent(evt);

                if (evt == LifecycleEventType.AFTER_NODE_START) {
                    Ignite ignite0 = Ignition.ignite(ignite.name());

                    assertNotNull(ignite0);
                    assertNotNull(ignite0.cluster().localNode());

                    done.set(true);
                }
            }
        };

        try {
            startGrid();

            assertTrue(done.get());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoErrors() throws Exception {
        bean = new LifeCycleBaseBean();

        startGrid();

        try {
            assertEquals(IgniteState.STARTED, G.state(getTestGridName()));

            assertEquals(1, bean.count(BEFORE_NODE_START));
            assertEquals(1, bean.count(AFTER_NODE_START));
            assertEquals(0, bean.count(BEFORE_NODE_STOP));
            assertEquals(0, bean.count(AFTER_NODE_STOP));
        }
        finally {
            stopAllGrids();
        }


        assertEquals(IgniteState.STOPPED, G.state(getTestGridName()));

        assertEquals(1, bean.count(BEFORE_NODE_START));
        assertEquals(1, bean.count(AFTER_NODE_START));
        assertEquals(1, bean.count(BEFORE_NODE_STOP));
        assertEquals(1, bean.count(AFTER_NODE_STOP));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridErrorBeforeStart() throws Exception {
        checkBeforeStart(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOtherErrorBeforeStart() throws Exception {
        checkBeforeStart(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridErrorAfterStart() throws Exception {
        checkAfterStart(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOtherErrorAfterStart() throws Exception {
        checkAfterStart(false);
    }

    /**
     * @param gridErr Grid error flag.
     * @throws Exception If failed.
     */
    private void checkBeforeStart(boolean gridErr) throws Exception {
        bean = new LifeCycleExceptionBean(BEFORE_NODE_START, gridErr);

        try {
            startGrid();

            assertTrue(false); // Should never get here.
        }
        catch (IgniteCheckedException expected) {
            info("Got expected exception: " + expected);

            assertEquals(IgniteState.STOPPED, G.state(getTestGridName()));
        }
        finally {
            stopAllGrids();
        }

        assertEquals(0, bean.count(BEFORE_NODE_START));
        assertEquals(0, bean.count(AFTER_NODE_START));
        assertEquals(0, bean.count(BEFORE_NODE_STOP));
        assertEquals(1, bean.count(AFTER_NODE_STOP));
    }

    /**
     * @param gridErr Grid error flag.
     * @throws Exception If failed.
     */
    private void checkAfterStart(boolean gridErr) throws Exception {
        bean = new LifeCycleExceptionBean(AFTER_NODE_START, gridErr);

        try {
            startGrid();

            assertTrue(false); // Should never get here.
        }
        catch (IgniteCheckedException expected) {
            info("Got expected exception: " + expected);

            assertEquals(IgniteState.STOPPED, G.state(getTestGridName()));
        }
        finally {
            stopAllGrids();
        }

        assertEquals(1, bean.count(BEFORE_NODE_START));
        assertEquals(0, bean.count(AFTER_NODE_START));
        assertEquals(1, bean.count(BEFORE_NODE_STOP));
        assertEquals(1, bean.count(AFTER_NODE_STOP));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridErrorBeforeStop() throws Exception {
        checkOnStop(BEFORE_NODE_STOP, true);

        assertEquals(1, bean.count(BEFORE_NODE_START));
        assertEquals(1, bean.count(AFTER_NODE_START));
        assertEquals(0, bean.count(BEFORE_NODE_STOP));
        assertEquals(1, bean.count(AFTER_NODE_STOP));
    }

    /**
     * @throws Exception If failed.
     */
    public void testOtherErrorBeforeStop() throws Exception {
        checkOnStop(BEFORE_NODE_STOP, false);

        assertEquals(1, bean.count(BEFORE_NODE_START));
        assertEquals(1, bean.count(AFTER_NODE_START));
        assertEquals(0, bean.count(BEFORE_NODE_STOP));
        assertEquals(1, bean.count(AFTER_NODE_STOP));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridErrorAfterStop() throws Exception {
        checkOnStop(AFTER_NODE_STOP, true);

        assertEquals(1, bean.count(BEFORE_NODE_START));
        assertEquals(1, bean.count(AFTER_NODE_START));
        assertEquals(1, bean.count(BEFORE_NODE_STOP));
        assertEquals(0, bean.count(AFTER_NODE_STOP));
    }

    /**
     * @throws Exception If failed.
     */
    public void testOtherErrorAfterStop() throws Exception {
        checkOnStop(AFTER_NODE_STOP, false);

        assertEquals(1, bean.count(BEFORE_NODE_START));
        assertEquals(1, bean.count(AFTER_NODE_START));
        assertEquals(1, bean.count(BEFORE_NODE_STOP));
        assertEquals(0, bean.count(AFTER_NODE_STOP));
    }

    /**
     * @param evt Error event.
     * @param gridErr Grid error flag.
     * @throws Exception If failed.
     */
    private void checkOnStop(LifecycleEventType evt, boolean gridErr) throws Exception {
        bean = new LifeCycleExceptionBean(evt, gridErr);

        try {
            startGrid();

            assertEquals(IgniteState.STARTED, G.state(getTestGridName()));
        }
        catch (IgniteCheckedException ignore) {
            assertTrue(false);
        }
        finally {
            try {
                stopAllGrids();

                assertEquals(IgniteState.STOPPED, G.state(getTestGridName()));
            }
            catch (Exception ignore) {
                assertTrue(false);
            }
        }
    }

    /**
     *
     */
    private static class LifeCycleBaseBean implements LifecycleBean {
        /** */
        private Map<LifecycleEventType, AtomicInteger> callsCntr =
            new EnumMap<>(LifecycleEventType.class);

        /**
         *
         */
        private LifeCycleBaseBean() {
            for (LifecycleEventType t : LifecycleEventType.values())
                callsCntr.put(t, new AtomicInteger());
        }

        /** {@inheritDoc} */
        @Override public void onLifecycleEvent(LifecycleEventType evt) {
            callsCntr.get(evt).incrementAndGet();
        }

        /**
         * @param t Event type.
         * @return Number of calls.
         */
        public int count(LifecycleEventType t) {
            return callsCntr.get(t).get();
        }
    }

    /**
     *
     */
    private static class LifeCycleExceptionBean extends LifeCycleBaseBean {
        /** */
        private LifecycleEventType errType;

        private boolean gridErr;

        /**
         * @param errType type of event to throw error.
         * @param gridErr {@code True} if {@link IgniteCheckedException}.
         */
        private LifeCycleExceptionBean(LifecycleEventType errType, boolean gridErr) {
            this.errType = errType;
            this.gridErr = gridErr;
        }

        /** {@inheritDoc} */
        @Override public void onLifecycleEvent(LifecycleEventType evt) {
            if (evt == errType) {
                if (gridErr)
                    throw new IgniteException("Expected exception for event: " + evt) {
                        @Override public void printStackTrace(PrintStream s) {
                            // No-op.
                        }

                        @Override public void printStackTrace(PrintWriter s) {
                            // No-op.
                        }
                    };
                else
                    throw new RuntimeException("Expected exception for event: " + evt) {
                        @Override public void printStackTrace(PrintStream s) {
                            // No-op.
                        }

                        @Override public void printStackTrace(PrintWriter s) {
                            // No-op.
                        }
                    };
            }

            super.onLifecycleEvent(evt);
        }
    }
}