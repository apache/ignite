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

package org.apache.ignite.configuration;

import java.util.regex.Pattern;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;

/**
 * Test covers cases where Ignite configuration,
 * or it's nested objects have default implementation of {@link Object#toString()}
 */
@WithSystemProperty(key = IGNITE_QUIET, value = "false")
public class IgniteConfigurationTest extends GridCommonAbstractTest {
    /** Error message to be prompted for ignite configuration */
    private static final String ASSERTION_ERROR_MESSAGE =
            "Ignite configuration log message contains objects with default #toString() implementation";

    /** Error message to be prompted for node start log message */
    private static final String NODE_START_ASSERTION_ERROR_MESSAGE =
            "Node start log message contains objects with default #toString() implementation";

    /** Contains not identity hash fragments ("@...") */
    public static final String CONTAINS_NOT_DEFAULT_TO_STRING = "(?!.*@[a-fA-F0-9]+).*";

    /** Pattern to check any object has default {@link Object#toString()} implementation */
    private static final Pattern ERROR_PATTERN =
            Pattern.compile("^(?=.*IgniteConfiguration \\[)"
                    + CONTAINS_NOT_DEFAULT_TO_STRING + "$");

    /** Pattern to check any object has default {@link Object#toString()} implementation */
    private static final Pattern NODE_START_ERROR_PATTERN =
            Pattern.compile("^(?=.*Node started with the following configuration \\[id=)"
                    + CONTAINS_NOT_DEFAULT_TO_STRING + "$");

    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /**
     * Checks that Ignite configuration log message contains no identity hash fragments ("@...").
     * This is a common heuristic to ensure all logged objects override {@link Object#toString()}.
     */
    @Test
    public void testIgniteConfigurationPrompt() throws Exception {
        LogListener igniteConfigurationLogListener = LogListener
                .matches(ERROR_PATTERN)
                .build();
        LogListener nodeStartIgniteConfigurationLogListener = LogListener
                .matches(NODE_START_ERROR_PATTERN)
                .build();
        listeningLog.registerListener(igniteConfigurationLogListener);
        listeningLog.registerListener(nodeStartIgniteConfigurationLogListener);
        try (IgniteEx ignored = startGrid(0)) {
            Assert.assertTrue(ASSERTION_ERROR_MESSAGE, igniteConfigurationLogListener.check());
            Assert.assertTrue(NODE_START_ASSERTION_ERROR_MESSAGE, nodeStartIgniteConfigurationLogListener.check());
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
                .setGridLogger(listeningLog);
    }
}
