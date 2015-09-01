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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiNoop;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Tests manager with {@link org.apache.ignite.spi.IgniteSpiNoop} SPI's.
 */
public class GridNoopManagerSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testEnabledManager() throws IgniteCheckedException {
        GridTestKernalContext ctx = new GridTestKernalContext(new GridStringLogger());

        assertTrue(new Manager(ctx, new Spi()).enabled());
        assertFalse(new Manager(ctx, new NoopSpi()).enabled());
        assertTrue(new Manager(ctx, new Spi(), new NoopSpi()).enabled());
        assertTrue(new Manager(ctx, new NoopSpi(), new Spi()).enabled());
    }

    /**
     *
     */
    private static class Manager extends GridManagerAdapter<IgniteSpi> {
        /**
         * @param ctx  Kernal context.
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
     *
     */
    private static interface TestSpi extends IgniteSpi {
        // No-op.
    }

    /**
     *
     */
    private static class Spi extends IgniteSpiAdapter implements TestSpi {
        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }
    }

    /**
     *
     */
    @IgniteSpiNoop
    private static class NoopSpi extends Spi {
        // No-op.
    }
}