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

import java.lang.management.ThreadInfo;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.client.thin.ReliableChannel.ASYNC_RUNNER_THREAD_NAME;
import static org.apache.ignite.internal.client.thin.TcpClientChannel.RECEIVER_THREAD_PREFIX;

/**
 * Test resource releasing by thin client.
 */
public class ThinClientPartitionAwarenessResourceReleaseTest extends ThinClientAbstractPartitionAwarenessTest {
    /**
     * Test that resources are correctly released after closing client with partition awareness.
     */
    @Test
    public void testResourcesReleasedAfterClientClosed() throws Exception {
        startGrids(2);

        initClient(getClientConfiguration(0, 1), 0, 1);

        ClientCache<Integer, Integer> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        assertFalse(channels[0].isClosed());
        assertFalse(channels[1].isClosed());
        assertEquals(1, threadsCount(ASYNC_RUNNER_THREAD_NAME));
        assertEquals(2, threadsCount(RECEIVER_THREAD_PREFIX));

        client.close();

        assertTrue(channels[0].isClosed());
        assertTrue(channels[1].isClosed());
        assertTrue(GridTestUtils.waitForCondition(() -> threadsCount(ASYNC_RUNNER_THREAD_NAME) == 0, 1_000L));
        assertTrue(GridTestUtils.waitForCondition(() -> threadsCount(RECEIVER_THREAD_PREFIX) == 0, 1_000L));
    }

    /**
     * Gets threads count with a given name.
     */
    private static int threadsCount(String name) {
        int cnt = 0;

        long[] threadIds = U.getThreadMx().getAllThreadIds();

        for (long id : threadIds) {
            ThreadInfo info = U.getThreadMx().getThreadInfo(id);

            if (info != null && info.getThreadState() != Thread.State.TERMINATED && info.getThreadName().startsWith(name))
                cnt++;
        }

        return cnt;
    }
}
