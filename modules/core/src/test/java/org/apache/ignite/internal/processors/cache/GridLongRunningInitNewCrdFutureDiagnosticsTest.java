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

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test class for diagnostics of long running {@link InitNewCoordinatorFuture}.
 */
public class GridLongRunningInitNewCrdFutureDiagnosticsTest extends GridCommonAbstractTest {
    /** Node with diagnostic logger. */
    private static final int NODE_WITH_DIAGNOSTIC_LOG = 2;

    /** Test logger. */
    private final ListeningTestLogger log = new ListeningTestLogger(false, GridAbstractTest.log);

    /** Test recording communication spi. */
    private TestRecordingCommunicationSpi testRecordingCommSpi;

    /** */
    private static final String DIAGNOSTIC_MESSAGE = "InitNewCoordinatorFuture waiting for GridDhtPartitionsSingleMessages from nodes=";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        testRecordingCommSpi = new TestRecordingCommunicationSpi();

        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(log)
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(testRecordingCommSpi);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        log.clearListeners();

        stopAllGrids();
    }

    /** */
    @Test
    public void receiveDiagnosticLogForLongRunningFuture() throws Exception {
        LogListener lsnr = expectLogEvent(DIAGNOSTIC_MESSAGE, 1);

        startGrid(0);
        startGrid(1);
        startGrid(NODE_WITH_DIAGNOSTIC_LOG);
        startGrid(3);

        testRecordingCommSpi.blockMessages(GridDhtPartitionsSingleMessage.class, getTestIgniteInstanceName(1));
        testRecordingCommSpi.blockMessages(GridDhtPartitionsSingleMessage.class, getTestIgniteInstanceName(NODE_WITH_DIAGNOSTIC_LOG));
        testRecordingCommSpi.blockMessages(GridDhtPartitionsSingleMessage.class, getTestIgniteInstanceName(3));

        stopGrid(0); // It makes PME future on node#1
        stopGrid(1); // It makes new crd init future inside PME future on node#2

        assertTrue(lsnr.check(getTestTimeout()));
    }

    /**
     * @param evtMsg Event message.
     * @param cnt Number of expected events.
     */
    private LogListener expectLogEvent(String evtMsg, int cnt) {
        LogListener lsnr = LogListener.matches(s -> s.startsWith(evtMsg)).atLeast(cnt).build();

        log.registerListener(lsnr);

        return lsnr;
    }

}
