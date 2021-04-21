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

package org.apache.ignite.internal.processors.failure;

import java.util.concurrent.atomic.AtomicBoolean;
import com.google.common.collect.ImmutableSet;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.failure.TestFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.failure.FailureProcessor.FAILURE_LOG_MSG;
import static org.apache.ignite.internal.processors.failure.FailureProcessor.IGNORED_FAILURE_LOG_MSG;
import static org.apache.ignite.internal.util.IgniteUtils.THREAD_DUMP_MSG;

/**
 * Tests log messages of failure processor.
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, value = "true")
public class FailureProcessorLoggingTest extends GridCommonAbstractTest {
    /** Test logger. */
    private static CustomTestLogger testLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestFailureHandler hnd = new TestFailureHandler(false);

        testLog = new CustomTestLogger(log);

        hnd.setIgnoredFailureTypes(ImmutableSet.of(FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT, FailureType.SYSTEM_WORKER_BLOCKED));

        cfg.setFailureHandler(hnd);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        testLog.expectedThreadDumpMessage(THREAD_DUMP_MSG);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        testLog.resetAllFlags();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        testLog.resetAllFlags();
    }

    /**
     * Tests log message for ignored failure types.
     */
    @Test
    public void testFailureProcessorLoggedIgnoredFailureTest() throws Exception {
        IgniteEx ignite = grid(0);

        testLog.expectedWarnMessage(IGNORED_FAILURE_LOG_MSG);

        ignite.context().failure().process(new FailureContext(FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT, new Throwable("Failure context error")));

        assertTrue(testLog.warnFlag().get());

        assertTrue(testLog.threadDumpWarnFlag().get());
    }

    /**
     * Tests log message for not ingnored failure types.
     */
    @Test
    public void testFailureProcessorLoggedFailureTest() throws Exception {
        IgniteEx ignite = grid(0);

        testLog.expectedErrorMessage(FAILURE_LOG_MSG);

        ignite.context().failure().process(new FailureContext(FailureType.SEGMENTATION, new Throwable("Failure context error")));

        assertTrue(testLog.errorFlag().get());

        assertTrue(testLog.threadDumpErrorFlag().get());
    }

    /**
     * Custom logger for checking if specified message and thread dump appeared at WARN and ERROR logging level.
     */
    private static class CustomTestLogger extends ListeningTestLogger {

        /** Warn flag. */
        private AtomicBoolean warnFlag = new AtomicBoolean(false);

        /** Error flag. */
        private AtomicBoolean errorFlag = new AtomicBoolean(false);

        /** Thread dump warn flag. */
        private AtomicBoolean threadDumpWarnFlag = new AtomicBoolean(false);

        /** Thread dump error flag. */
        private AtomicBoolean threadDumpErrorFlag = new AtomicBoolean(false);

        /** Expected warn message. */
        private String expWarnMsg;

        /** Expected error message. */
        private String expErrorMsg;

        /** Thread dump message. */
        private String expThreadDumpMsg;

        /**
         * @param echo Echo.
         */
        public CustomTestLogger(@Nullable IgniteLogger echo) {
            super(echo);
        }

        /** {@inheritDoc} */
        @Override public void warning(String msg, @Nullable Throwable t) {
            super.warning(msg, t);

            if (expWarnMsg != null && msg.contains(expWarnMsg))
                warnFlag.set(true);

            if (expThreadDumpMsg != null && msg.contains(expThreadDumpMsg))
                threadDumpWarnFlag.set(true);
        }

        /** {@inheritDoc} */
        @Override public void error(String msg, @Nullable Throwable t) {
            super.error(msg, t);

            if (expErrorMsg != null && msg.contains(expErrorMsg))
                errorFlag.set(true);

            if (expThreadDumpMsg != null && msg.contains(expThreadDumpMsg))
                threadDumpErrorFlag.set(true);
        }

        /**
         * Reset warn flag.
         */
        public void resetWarnFlag() {
            warnFlag.set(false);
        }

        /**
         * Reset error flag.
         */
        public void resetErrorFlag() {
            errorFlag.set(false);
        }

        /**
         * Reset thread dump warn flag.
         */
        public void resetThreadDumpWarnFlag() {
            threadDumpWarnFlag.set(false);
        }

        /**
         * Reset thread dump error flag.
         */
        public void resetThreadDumpErrorFlag() {
            threadDumpErrorFlag.set(false);
        }

        /**
         * Reset all flags.
         */
        public void resetAllFlags() {
            resetWarnFlag();
            resetErrorFlag();
            resetThreadDumpWarnFlag();
            resetThreadDumpErrorFlag();
        }

        /**
         * @param expWarnMsg New expected warn message.
         */
        public void expectedWarnMessage(String expWarnMsg) {
            this.expWarnMsg = expWarnMsg;
        }

        /**
         * @param expErrorMsg New expected error message.
         */
        public void expectedErrorMessage(String expErrorMsg) {
            this.expErrorMsg = expErrorMsg;
        }

        /**
         * @param expThreadDumpMsg New expected thread dump message.
         */
        public void expectedThreadDumpMessage(String expThreadDumpMsg) {
            this.expThreadDumpMsg = expThreadDumpMsg;
        }

        /**
         * @return Warn flag.
         */
        public AtomicBoolean warnFlag() {
            return warnFlag;
        }

        /**
         * @return Error flag.
         */
        public AtomicBoolean errorFlag() {
            return errorFlag;
        }

        /**
         * @return Thread dump warn flag.
         */
        public AtomicBoolean threadDumpWarnFlag() {
            return threadDumpWarnFlag;
        }

        /**
         * @return Thread dump error flag.
         */
        public AtomicBoolean threadDumpErrorFlag() {
            return threadDumpErrorFlag;
        }
    }

}
