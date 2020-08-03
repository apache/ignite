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
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for {@link ExchangeLatchManager} functionality when latch coordinator is failed.
 */
public class ExchangeLatchManagerTest extends GridCommonAbstractTest {
    /** */
    private static final String LATCH_NAME = "test";

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
