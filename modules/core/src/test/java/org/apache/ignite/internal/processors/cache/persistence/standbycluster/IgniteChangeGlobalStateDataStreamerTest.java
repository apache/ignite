/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.standbycluster;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cluster.ClusterState;
import org.junit.Test;

/**
 *
 */
public class IgniteChangeGlobalStateDataStreamerTest extends IgniteChangeGlobalStateAbstractTest {
    /** {@inheritDoc} */
    @Override protected int backUpNodes() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override protected int backUpClientNodes() {
        return 0;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeActivateAndActivateDataStreamer() throws Exception {
        Ignite ig1 = primary(0);
        Ignite ig2 = primary(1);
        Ignite ig3 = primary(2);

        Ignite ig1C = primaryClient(0);
        Ignite ig2C = primaryClient(1);
        Ignite ig3C = primaryClient(2);

        assertTrue(ig1.cluster().state().active());
        assertTrue(ig2.cluster().state().active());
        assertTrue(ig3.cluster().state().active());

        assertTrue(ig1C.cluster().state().active());
        assertTrue(ig2C.cluster().state().active());
        assertTrue(ig3C.cluster().state().active());

        String cacheName = "myStreamCache";

        ig2C.getOrCreateCache(cacheName);

        try (IgniteDataStreamer<Integer, String> stmr = ig1.dataStreamer(cacheName)) {
            for (int i = 0; i < 100; i++)
                stmr.addData(i, Integer.toString(i));
        }

        ig2C.cluster().state(ClusterState.INACTIVE);

        assertTrue(!ig1.cluster().state().active());
        assertTrue(!ig2.cluster().state().active());
        assertTrue(!ig3.cluster().state().active());

        assertTrue(!ig1C.cluster().state().active());
        assertTrue(!ig2C.cluster().state().active());
        assertTrue(!ig3C.cluster().state().active());

        boolean fail = false;

        try {
            IgniteDataStreamer<String, String> strm2 = ig2.dataStreamer(cacheName);
        }
        catch (Exception e) {
            fail = true;

            assertTrue(e.getMessage().contains("Can not perform the operation because the cluster is inactive."));
        }

        if (!fail)
            fail("exception was not throw");

        ig3C.cluster().state(ClusterState.ACTIVE);

        assertTrue(ig1.cluster().state().active());
        assertTrue(ig2.cluster().state().active());
        assertTrue(ig3.cluster().state().active());

        assertTrue(ig1C.cluster().state().active());
        assertTrue(ig2C.cluster().state().active());
        assertTrue(ig3C.cluster().state().active());

        try (IgniteDataStreamer<Integer, String> stmr2 = ig2.dataStreamer(cacheName)) {
            for (int i = 100; i < 200; i++)
                stmr2.addData(i, Integer.toString(i));
        }

        IgniteCache<Integer, String> cache = ig3.cache(cacheName);

        for (int i = 0; i < 200; i++)
            assertEquals(String.valueOf(i), cache.get(i));
    }
}
