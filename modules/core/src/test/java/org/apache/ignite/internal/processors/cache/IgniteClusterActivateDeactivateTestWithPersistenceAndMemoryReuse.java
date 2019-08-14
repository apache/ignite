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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.junit.Assume;

/**
 *
 */
public class IgniteClusterActivateDeactivateTestWithPersistenceAndMemoryReuse extends
    IgniteClusterActivateDeactivateTestWithPersistence {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_REUSE_MEMORY_ON_DEACTIVATE, "true");

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        System.clearProperty(IgniteSystemProperties.IGNITE_REUSE_MEMORY_ON_DEACTIVATE);
    }

    /** {@inheritDoc} */
    @Override public void testReActivateSimple_5_Servers_4_Clients_FromClient() throws Exception {
        Assume.assumeFalse("https://ggsystems.atlassian.net/browse/GG-22712", MvccFeatureChecker.forcedMvcc());

        super.testReActivateSimple_5_Servers_4_Clients_FromClient();
    }

    /** {@inheritDoc} */
    @Override public void testReActivateSimple_5_Servers_4_Clients_FromServer() throws Exception {
        Assume.assumeFalse("https://ggsystems.atlassian.net/browse/GG-22712", MvccFeatureChecker.forcedMvcc());

        super.testReActivateSimple_5_Servers_4_Clients_FromServer();
    }
}
