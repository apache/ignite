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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.junit.Test;

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
    @Test
    @Override public void testDeactivateDuringEvictionAndRebalance() throws Exception {
        if (MvccFeatureChecker.forcedMvcc())
            fail("https://issues.apache.org/jira/browse/IGNITE-10788");

        super.testDeactivateDuringEvictionAndRebalance();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testDeactivateInactiveCluster() throws Exception {
        if (MvccFeatureChecker.forcedMvcc())
            fail("https://issues.apache.org/jira/browse/IGNITE-10788");

        super.testDeactivateInactiveCluster();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testReActivateSimple_5_Servers_4_Clients_FromServer() throws Exception {
        if (MvccFeatureChecker.forcedMvcc())
            fail("https://issues.apache.org/jira/browse/IGNITE-10750");

        super.testReActivateSimple_5_Servers_4_Clients_FromServer();
    }
}
