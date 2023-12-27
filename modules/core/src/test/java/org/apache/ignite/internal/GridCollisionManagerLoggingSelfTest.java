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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.managers.collision.GridCollisionManager;
import org.apache.ignite.spi.collision.noop.NoopCollisionSpi;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests for making sure that {@link GridCollisionManager} logs about specific conditions at correct levels,
 * messages are correct and so on.
 */
public class GridCollisionManagerLoggingSelfTest {
    /** Logger used to intercept and inspect logged messages. */
    private final IgniteLogger logger = mock(IgniteLogger.class);

    /**
     * Inits logger mock to make it behave nicely (like return a non-null logger from
     * <tt>IgniteLogger#getLogger()</tt> method).
     */
    @Before
    public void initLoggerMock() {
        doReturn(logger).when(logger).getLogger(any());
    }

    /**
     * Makes sure that, given collision resolution is disabled, during start up the corresponding
     * message is logged as INFO, not as WARN.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void collisionResolutionDisabledMessageShouldBeLoggedAtInfoLevel() throws Exception {
        GridCollisionManager mgr = new GridCollisionManager(collisionResolutionDisabledContext());

        mgr.start();

        verify(logger).info("Collision resolution is disabled (all jobs will be activated upon arrival).");
    }

    /**
     * Builds context without collision resolution.
     *
     * @return context without collision resolution
     */
    private GridTestKernalContext collisionResolutionDisabledContext() {
        GridTestKernalContext ctx = new GridTestKernalContext(logger);
        ctx.config().setCollisionSpi(new NoopCollisionSpi());
        return ctx;
    }
}
