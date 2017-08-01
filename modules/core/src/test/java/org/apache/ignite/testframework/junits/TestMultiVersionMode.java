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

package org.apache.ignite.testframework.junits;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.security.compatibility.TestCompatibilityPluginProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class TestMultiVersionMode extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();
        cfg.setPeerClassLoadingEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        TestCompatibilityPluginProvider.enable();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        TestCompatibilityPluginProvider.disable();
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** */
    public void testJoinMultiVersionTopology() throws Exception {
        try {
            startGrid(0);

            startGrid("testMultiVersion", "2.0.0", null, new ConfigurationPostProcessor());
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private static class ConfigurationPostProcessor implements IgniteInClosure<IgniteConfiguration> {
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setLateAffinityAssignment(true);
        }
    }
}
