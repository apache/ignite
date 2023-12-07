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

package org.apache.ignite.internal.processors.resource;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.resources.ServiceContextResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for injected service context.
 */
public class GridServiceContextInjectionSelfTest extends GridCommonAbstractTest {
    /** Service name. */
    private static final String DUMMY_SERVICE = "dummy";

    /** Compatibiity service name. */
    private static final String COMPATIBILITY_SERVICE = "compatibility";

    /** Error handler. */
    private static final ErrorHandler errHnd = new ErrorHandler();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        grid(0).context().service().cancelAll();

        errHnd.reset();
    }

    /**
     * Checks that the context is correctly injected into the service.
     */
    @Test
    public void testInjection() {
        grid(0).services().deployClusterSingleton(DUMMY_SERVICE, new DummyServiceImpl());

        doLifeCycleTest(DUMMY_SERVICE);
    }

    /**
     * Ensures that the injected context instance is the same as obtained in legacy methods.
     */
    @Test
    public void testCompatibility() {
        grid(0).services().deployClusterSingleton(COMPATIBILITY_SERVICE, new CompatibilityServiceImpl());

        doLifeCycleTest(COMPATIBILITY_SERVICE);
    }

    /**
     * @param svcName Service name.
     */
    private void doLifeCycleTest(String svcName) {
        DummyService svc = grid(1).services().serviceProxy(svcName, DummyService.class, false);

        svc.method();

        grid(0).services().cancel(svcName);

        errHnd.validate();
    }

    /**
     * Dummy service.
     */
    public interface DummyService {
        /** */
        public void method();
    }

    /** */
    public static class DummyServiceImpl implements DummyService, Service {
        /** */
        private static final long serialVersionUID = 0L;

        /** Service context. */
        @ServiceContextResource
        private ServiceContext ctxField;

        /** Service context. */
        private ServiceContext ctxSetter;

        /** Service initialized flag. */
        private volatile boolean initialized;

        /** Service executed flag. */
        private volatile boolean executed;

        /** Service canceled flag. */
        private volatile boolean canceled;

        /**
         * @param ctx Service context.
         */
        @ServiceContextResource
        private void serviceContext(ServiceContext ctx) {
            ctxSetter = ctx;
        }

        /** {@inheritDoc} */
        @Override public void init() {
            errHnd.ensure(ctxSetter != null);
            errHnd.ensure(ctxSetter == ctxField);

            errHnd.ensure(!initialized);
            errHnd.ensure(!executed);
            errHnd.ensure(!canceled);

            initialized = true;
        }

        /** {@inheritDoc} */
        @Override public void execute() {
            errHnd.ensure(ctxSetter != null);
            errHnd.ensure(ctxSetter == ctxField);

            errHnd.ensure(initialized);
            errHnd.ensure(!executed);
            errHnd.ensure(!canceled);

            executed = true;

            try {
                GridTestUtils.waitForCondition(() -> ctxSetter.isCancelled(), GridTestUtils.DFLT_TEST_TIMEOUT);
            }
            catch (IgniteInterruptedCheckedException ignore) {
                // Execution interrupted when service is cancelled.
            }

            errHnd.ensure(ctxSetter.isCancelled());
            errHnd.ensure(canceled);
        }

        /** {@inheritDoc} */
        @Override public void method() {
            assertTrue(initialized);
            assertTrue(executed);
            assertFalse(canceled);
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            errHnd.ensure(ctxSetter != null);
            errHnd.ensure(ctxSetter == ctxField);

            errHnd.ensure(initialized);
            errHnd.ensure(executed);
            errHnd.ensure(!canceled);

            canceled = true;
        }
    }

    /**
     * Service for checking compatibility of the injected service context.
     */
    public static class CompatibilityServiceImpl implements DummyService, Service {
        /** */
        private static final long serialVersionUID = 0L;

        /** Service context. */
        @ServiceContextResource
        private ServiceContext ctx;

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            errHnd.ensure(ctx == this.ctx);
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) {
            errHnd.ensure(ctx == this.ctx);
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) {
            errHnd.ensure(ctx == this.ctx);
        }

        /** {@inheritDoc} */
        @Override public void method() {
            // No-op.
        }
    }

    /** */
    private static class ErrorHandler {
        /** Error reference. */
        private final AtomicReference<AssertionError> errRef = new AtomicReference<>();

        /**
         * @param condition Assertion condition to check.
         */
        void ensure(boolean condition) {
            if (!condition) {
                AssertionError err = new AssertionError();

                errRef.compareAndSet(null, err);

                throw err;
            }
        }

        /**
         * @throws AssertionError If there was an error.
         */
        void validate() throws AssertionError {
            Error err = errRef.get();

            if (errRef.get() != null)
                throw err;
        }

        /** */
        void reset() {
            errRef.set(null);
        }
    }
}
