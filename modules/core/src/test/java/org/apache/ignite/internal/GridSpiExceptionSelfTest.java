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

import java.util.Collection;
import org.apache.ignite.GridTestTask;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.deployment.DeploymentListener;
import org.apache.ignite.spi.deployment.DeploymentResource;
import org.apache.ignite.spi.deployment.DeploymentSpi;
import org.apache.ignite.spi.eventstorage.EventStorageSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.Nullable;

/**
 * Tests exceptions that are thrown by event storage and deployment spi.
 */
@GridCommonTest(group = "Kernal Self")
public class GridSpiExceptionSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_MSG = "Test exception message";

    /** */
    public GridSpiExceptionSelfTest() {
        super(/*start Grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setEventStorageSpi(new GridTestRuntimeExceptionSpi());
        cfg.setDeploymentSpi(new GridTestCheckedExceptionSpi());

        // Disable cache since it can deploy some classes during start process.
        cfg.setCacheConfiguration();

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSpiFail() throws Exception {
        Ignite ignite = startGrid();

        try {
            try {
                ignite.events().localQuery(F.<Event>alwaysTrue());

                assert false : "Exception should be thrown";
            }
            catch (IgniteException e) {
                assert e.getMessage().startsWith(TEST_MSG) : "Wrong exception message." + e.getMessage();
            }

            try {
                ignite.compute().localDeployTask(GridTestTask.class, GridTestTask.class.getClassLoader());

                assert false : "Exception should be thrown";
            }
            catch (IgniteException e) {
                assertTrue(e.getCause() instanceof  IgniteCheckedException);

                Throwable err = e.getCause().getCause();

                assert err instanceof GridTestSpiException : "Wrong cause exception type. " + e;

                assert err.getMessage().startsWith(TEST_MSG) : "Wrong exception message." + e.getMessage();
            }
        }
        finally {
            stopGrid();
        }
    }

    /**
     * Test event storage spi that throws an exception on try to query local events.
     */
    @IgniteSpiMultipleInstancesSupport(true)
    private static class GridTestRuntimeExceptionSpi extends IgniteSpiAdapter implements EventStorageSpi {
        /** {@inheritDoc} */
        @Override public void spiStart(String gridName) throws IgniteSpiException {
            startStopwatch();
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public <T extends Event> Collection<T> localEvents(IgnitePredicate<T> p) {
            throw new IgniteException(TEST_MSG);
        }

        /** {@inheritDoc} */
        @Override public void record(Event evt) throws IgniteSpiException {
            // No-op.
        }
    }

    /**
     * Test deployment spi that throws an exception on try to register any class.
     */
    @IgniteSpiMultipleInstancesSupport(true)
    private static class GridTestCheckedExceptionSpi extends IgniteSpiAdapter implements DeploymentSpi {
        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
            startStopwatch();
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Nullable @Override public DeploymentResource findResource(String rsrcName) {
            // No-op.
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean register(ClassLoader ldr, Class<?> rsrc) throws IgniteSpiException {
            throw new GridTestSpiException(TEST_MSG);
        }

        /** {@inheritDoc} */
        @Override public boolean unregister(String rsrcName) {
            // No-op.
            return false;
        }

        /** {@inheritDoc} */
        @Override public void setListener(DeploymentListener lsnr) {
            // No-op.
        }
    }

    /**
     * Test spi exception.
     */
    private static class GridTestSpiException extends IgniteSpiException {
        /**
         * @param msg Error message.
         */
        GridTestSpiException(String msg) {
            super(msg);
        }

        /**
         * @param msg Error message.
         * @param cause Error cause.
         */
        GridTestSpiException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }
}