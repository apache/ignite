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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.client.ClientCollectionConfiguration;
import org.apache.ignite.client.ClientIgniteSet;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.junit.Test;

/**
 * Tests client set.
 * Partition awareness tests are in {@link ThinClientPartitionAwarenessStableTopologyTest#testIgniteSet()}.
 */
// TODO: Test custom objects
// TODO: Test behavior when removed or does not exist - match thick API.
// TODO: Test partition awareness.
public class IgniteSetTest extends AbstractThinClientTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected ClientConfiguration getClientConfiguration() {
        return super.getClientConfiguration().setPartitionAwarenessEnabled(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    @Test
    public void testBasicUsage() {
        try (IgniteClient client = startClient(0)) {
            ClientIgniteSet<String> set = client.set("testBasicUsage", new ClientCollectionConfiguration());

            set.add("foo");
            set.add("bar");

            assertTrue(set.contains("foo"));
            assertTrue(set.contains("bar"));
            assertFalse(set.contains("baz"));

            set.remove("foo");
            assertFalse(set.contains("foo"));

            assertEquals(1, set.size());
        }
    }
}
