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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch;

import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for {@link ExchangeLatchManager} functionality when latch coordinator is failed.
 */
public class ExchangeLatchManagerTest extends GridCommonAbstractTest {
    /** */
    private static final String LATCH_NAME = "test";

    /** Message are meaning that node getting a stale acknowledge message. */
    private static final String STALE_ACK_LOG_MSG = "Latch for this acknowledge is completed or never existed";

    /** Message happens when assertion was broken. */
    public static final Pattern ERROR_MSG = Pattern.compile("An error occurred processing the message.*"
        + LatchAckMessage.class.getSimpleName());

    /** Grid logger. */
    public ListeningTestLogger gridLogger;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(gridLogger)
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Checks reaction of latch on stale acknowledge from new coordinator.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExcessAcknowledgeForNewCoordinator() throws Exception {
        gridLogger = new ListeningTestLogger(log);

        LogListener staleMessageLsnr = LogListener.matches(STALE_ACK_LOG_MSG).build();
        LogListener errorLsnr = LogListener.matches(ERROR_MSG).build();

        IgniteEx ignite0 = startGrids(3);

        awaitPartitionMapExchange();

        TestRecordingCommunicationSpi spi0 = TestRecordingCommunicationSpi.spi(ignite0);

        spi0.blockMessages((node, msg) ->
            msg instanceof LatchAckMessage && node.order() == 2);

        spi0.record((node, msg) ->
            msg instanceof LatchAckMessage && node.order() == 3);

        Ignite ignite1 = G.allGrids().stream().filter(node -> node.cluster().localNode().order() == 2).findAny().get();

        assertNotNull("Could not find node with second order.", ignite1);

        TestRecordingCommunicationSpi spi1 = TestRecordingCommunicationSpi.spi(ignite1);

        spi1.blockMessages((node, msg) -> {
            if (msg instanceof LatchAckMessage && node.order() == 3) {
                LatchAckMessage ack = (LatchAckMessage)msg;

                return ack.topVer().topologyVersion() == 3;
            }

            return false;
        });

        IgniteInternalFuture exchangeDoingFut = GridTestUtils.runAsync(() ->
            ignite0.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
        );

        spi0.waitForBlocked();
        spi0.waitForRecorded();

        ignite0.close();

        spi1.waitForBlocked();

        awaitPartitionMapExchange();

        assertTrue(exchangeDoingFut.isDone());

        gridLogger.registerAllListeners(errorLsnr, staleMessageLsnr);

        spi1.stopBlock();

        assertTrue(GridTestUtils.waitForCondition(() ->
            staleMessageLsnr.check(), 10_000));

        assertFalse(errorLsnr.check());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldCorrectlyExecuteLatchWhenCrdCreatedLast() throws Exception {
        IgniteEx crd = startGrid(0);
        IgniteEx ignite1 = startGrid(1);
        startGrid(2);

        //Version which is greater than current.
        AffinityTopologyVersion nextVer = new AffinityTopologyVersion(crd.cluster().topologyVersion() + 1, 0);

        //Send ack message from client latch before server latch would be created.
        ignite1.context().io().sendToGridTopic(
            crd.localNode(),
            GridTopic.TOPIC_EXCHANGE,
            new LatchAckMessage(
                LATCH_NAME, nextVer, false
            ), GridIoPolicy.SYSTEM_POOL
        );

        //Current version increase up to nextVer after this event.
        stopGrid(2);

        //This latch expected ack only from this node and from ignite1 which already sent it.
        Latch latchCrdOther = latchManager(0).getOrCreate(LATCH_NAME, nextVer);

        latchCrdOther.countDown();
        latchCrdOther.await(1, TimeUnit.SECONDS);
    }

    /**
     * Extract latch manager.
     *
     * @param nodeId Node id from which latch should be extracted.
     * @return Latch manager.
     */
    private ExchangeLatchManager latchManager(int nodeId) {
        return grid(nodeId).context().cache().context().exchange().latch();
    }
}
