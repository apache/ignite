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

package org.apache.ignite.spi.checkpoint.noop;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.regex.Pattern;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Tests for logging concerns of {@link NoopCheckpointSpi}: how and when it logs or does not log.
 */
public class NoopCheckpointSpiLoggingTest {
    /** An empty byte array. */
    private static final byte[] EMPTY_ARRAY = new byte[0];

    /** The tested SPI instance. */
    private final NoopCheckpointSpi checkpointSpi = new NoopCheckpointSpi();

    /** Logger used to intercept and inspect logged messages. */
    private final ListeningTestLogger logger = new ListeningTestLogger();

    /** Logging listener used throught the test. */
    private LogListener listener;

    /**
     * Injects logger into SPI instance to prepare it for work.
     *
     * @throws Exception if something goes wrong
     */
    @Before
    public void injectLogger() throws Exception {
        for (Field field : NoopCheckpointSpi.class.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers()) && field.getType() == IgniteLogger.class) {
                field.setAccessible(true);
                field.set(checkpointSpi, logger);
            }
        }
    }

    /**
     * Makes sure that 'checkpoints are disabled' is not logged at startup.
     */
    @Test
    public void shouldNotLogAboutCheckpointsBeingDisabledAtStartup() {
        LogListener listener = LogListener.matches(Pattern.compile("Checkpoints are disabled.+"))
                .atMost(0)
                .build();
        logger.registerListener(listener);

        checkpointSpi.spiStart("test");

        assertTrue(listener.check());
    }

    /**
     * Makes sure that 'checkpoints are disabled' is logged at a first attempt to save a checkoint.
     */
    @Test
    public void shouldLogAboutCheckpointsBeingDisabledOnCheckpointSave() {
        registerListenerExpectingExactlyOneCheckpointsDisabledMessage();

        checkpointSpi.saveCheckpoint("point", EMPTY_ARRAY, 1, true);

        verifyExactlyOneLogMessageSeen();
    }

    /**
     * Makes sure that exactly one expected message is seen by the logger.
     */
    private void verifyExactlyOneLogMessageSeen() {
        assertTrue(listener.check());
    }

    /**
     * Registers a listener for 'checkpoints are disabled' message that expects exactly one such message.
     */
    @NotNull
    private LogListener registerListenerExpectingExactlyOneCheckpointsDisabledMessage() {
        listener = LogListener
                .matches("Checkpoints are disabled (to enable configure any GridCheckpointSpi implementation)")
                .times(1)
                .build();
        logger.registerListener(listener);
        return listener;
    }

    /**
     * Makes sure that 'checkpoints are disabled' is logged at a first attempt to load a checkoint.
     */
    @Test
    public void shouldLogAboutCheckpointsBeingDisabledOnCheckpointLoad() {
        registerListenerExpectingExactlyOneCheckpointsDisabledMessage();

        checkpointSpi.loadCheckpoint("point");

        verifyExactlyOneLogMessageSeen();
    }

    /**
     * Makes sure that 'checkpoints are disabled' is logged at a first attempt to remove a checkoint.
     */
    @Test
    public void shouldLogAboutCheckpointsBeingDisabledOnCheckpointRemoval() {
        registerListenerExpectingExactlyOneCheckpointsDisabledMessage();

        checkpointSpi.removeCheckpoint("point");

        verifyExactlyOneLogMessageSeen();
    }

    /**
     * Makes sure that 'checkpoints are disabled' is logged at most once.
     */
    @Test
    public void shouldOnlyLogOnceAboutCheckointsBeingDisabled() {
        registerListenerExpectingExactlyOneCheckpointsDisabledMessage();

        // invoke every potentially logging method twice
        checkpointSpi.saveCheckpoint("point", EMPTY_ARRAY, 1, true);
        checkpointSpi.saveCheckpoint("point", EMPTY_ARRAY, 1, true);
        checkpointSpi.loadCheckpoint("point");
        checkpointSpi.loadCheckpoint("point");
        checkpointSpi.removeCheckpoint("point");
        checkpointSpi.removeCheckpoint("point");

        verifyExactlyOneLogMessageSeen();
    }
}
