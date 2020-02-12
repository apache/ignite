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

package org.apache.ignite.internal.processors.continuous;

import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Sanity test to verify there are no unnecessary messages on node start.
 */
public class IgniteNoCustomEventsOnNodeStart extends GridCommonAbstractTest {
    /** */
    private static volatile boolean failed;

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoCustomEventsOnStart() throws Exception {
        failed = false;

        for (int i = 0; i < 5; i++) {
            if (i % 2 == 1)
                startClientGrid(i);
            else
                startGrid(i);
        }

        assertFalse(failed);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     *
     */
    static class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) {
            if (GridTestUtils.getFieldValue(msg, "delegate") instanceof CacheAffinityChangeMessage)
                return;

            failed = true;

            fail("Should not be called: " + msg);
        }
    }
}
