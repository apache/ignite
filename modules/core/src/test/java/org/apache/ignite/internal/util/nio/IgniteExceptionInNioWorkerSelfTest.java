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

package org.apache.ignite.internal.util.nio;

import java.util.UUID;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteExceptionInNioWorkerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration("cache");

        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBrokenMessage() throws Exception {
        startGrids(GRID_CNT);

        try {
            IgniteKernal kernal = (IgniteKernal)ignite(0);

            UUID nodeId = ignite(1).cluster().localNode().id();

            // This should trigger a failure in a NIO thread.
            kernal.context().io().sendToCustomTopic(nodeId, GridTopic.TOPIC_CACHE.topic("cache"), new BrokenMessage(), (byte)0);

            for (int i = 0; i < 100; i++)
                ignite(0).cache("cache").put(i, i);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class BrokenMessage extends AffinityTopologyVersion {
        /** */
        private boolean fail = true;

        /** {@inheritDoc} */
        @Override public short directType() {
            if (fail) {
                fail = false;

                return (byte)242;
            }

            return super.directType();
        }
    }
}
