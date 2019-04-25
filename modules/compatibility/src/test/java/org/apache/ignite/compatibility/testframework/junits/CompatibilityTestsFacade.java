/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compatibility.testframework.junits;

import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Provides useful methods for compatibility tests.
 */
public class CompatibilityTestsFacade {
    /** */
    private static final TestsMethodsFacade DELEGATE = new TestsMethodsFacade();

    /**
     * Gets default tests configuration.
     *
     * @return Default tests Ignite configuration.
     * @throws Exception In case of an error.
     */
    public static IgniteConfiguration getConfiguration() throws Exception {
        return DELEGATE.getConfiguration();
    }

    /** */
    private static class TestsMethodsFacade extends IgniteCompatibilityAbstractTest {
        /** {@inheritDoc} */
        @Override public IgniteConfiguration getConfiguration() throws Exception {
            return super.getConfiguration();
        }
    }
}
