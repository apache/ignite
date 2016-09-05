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

package org.apache.ignite.internal.managers;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Test Managers to add and remove user message listener.
 */
public class GridManagerUserMessageListenerSelfTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddUserMessageListener() throws Exception {
        Manager mgr = new Manager(grid().context(), new Spi());

        mgr.start();

        mgr.onKernalStart();

        assertTrue(mgr.enabled());
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveUserMessageListener() throws Exception {
        Manager mgr = new Manager(grid().context(), new Spi());

        assertTrue(mgr.enabled());

        mgr.onKernalStart();

        mgr.onKernalStop(false);

        mgr.stop(false);

        assertTrue(mgr.enabled());
    }

    /**
     *
     */
    private static class Manager extends GridManagerAdapter<IgniteSpi> {
        /**
         * @param ctx Kernal context.
         * @param spis Specific SPI instance.
         */
        protected Manager(GridKernalContext ctx, IgniteSpi... spis) {
            super(ctx, spis);
        }

        /** {@inheritDoc} */
        @Override public void start() throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void stop(boolean cancel) throws IgniteCheckedException {
            // No-op.
        }
    }

    /**
     * Test Spi.
     */
    private static interface TestSpi extends IgniteSpi {
        // No-op.
    }

    /**
     * Spi
     */
    private static class Spi extends IgniteSpiAdapter implements TestSpi {
        /** Ignite Spi Context. **/
        private IgniteSpiContext spiCtx;

        /** Test message topic. **/
        private String TEST_TOPIC = "test_topic";

        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }

        @Override public void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
            this.spiCtx = spiCtx;

            spiCtx.addUserMessageListener(TEST_TOPIC, new IgniteBiPredicate<UUID, Object>() {
                @Override public boolean apply(UUID uuid, Object o) {
                    return true;
                }
            });

        }

        @Override public void onContextDestroyed0() {
            spiCtx.removeUserMessageListener(TEST_TOPIC, new IgniteBiPredicate<UUID, Object>() {
                @Override public boolean apply(UUID uuid, Object o) {
                    return true;
                }
            });
        }
    }
}
