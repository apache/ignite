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

package org.apache.ignite.internal.processors.compute;

import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ExecutorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests custom executor configuration.
 */
public class IgniteComputeCustomExecutorConfigurationSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConfigurations() throws Exception {
        try {
            checkStartWithInvalidConfiguration(getConfiguration("node0")
                .setExecutorConfiguration(new ExecutorConfiguration()));

            checkStartWithInvalidConfiguration(getConfiguration("node0")
                .setExecutorConfiguration(new ExecutorConfiguration("")));

            checkStartWithInvalidConfiguration(getConfiguration("node0")
                .setExecutorConfiguration(new ExecutorConfiguration("exec").setSize(-1)));

            checkStartWithInvalidConfiguration(getConfiguration("node0")
                .setExecutorConfiguration(new ExecutorConfiguration("exec").setSize(0)));
        }
        finally {
            Ignition.stopAll(true);
        }
    }

    /**
     * @param cfg Ignite configuration.
     * @throws Exception If failed.
     */
    private void checkStartWithInvalidConfiguration(IgniteConfiguration cfg) throws Exception {
        try {
            Ignition.start(cfg);

            fail("Node start must fail.");
        }
        catch (IgniteException e) {
            // No-op.
        }
    }
}
