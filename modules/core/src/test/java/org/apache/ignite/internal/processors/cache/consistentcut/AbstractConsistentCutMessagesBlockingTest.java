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

package org.apache.ignite.internal.processors.cache.consistentcut;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;

/** */
public abstract class AbstractConsistentCutMessagesBlockingTest extends AbstractConsistentCutBlockingTest {
    /** */
    protected static volatile String blkMsgCls;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCommunicationSpi(new BlockingCommunicationSpi());

        return cfg;
    }

    /** */
    protected List<String> messagesNoBackups(boolean onePhaseCommit) {
        List<String> msgCls = new ArrayList<>();

        if (onePhaseCommit) {
            msgCls.add(GridNearTxPrepareRequest.class.getSimpleName());
            msgCls.add(GridNearTxPrepareResponse.class.getSimpleName());
        }
        else {
            msgCls.add(GridNearTxPrepareRequest.class.getSimpleName());
            msgCls.add(GridNearTxPrepareResponse.class.getSimpleName());
            msgCls.add(GridNearTxFinishRequest.class.getSimpleName());
            msgCls.add(GridNearTxFinishResponse.class.getSimpleName());
        }

        return msgCls;
    }

    /** */
    protected static List<String> messages() {
        List<String> msgCls = new ArrayList<>();

        msgCls.add(GridNearTxPrepareRequest.class.getSimpleName());
        msgCls.add(GridNearTxPrepareResponse.class.getSimpleName());
        msgCls.add(GridNearTxFinishRequest.class.getSimpleName());
        msgCls.add(GridNearTxPrepareResponse.class.getSimpleName());
        msgCls.add(GridDhtTxPrepareRequest.class.getSimpleName());
        msgCls.add(GridDhtTxPrepareResponse.class.getSimpleName());
        msgCls.add(GridDhtTxFinishRequest.class.getSimpleName());
        msgCls.add(GridDhtTxFinishResponse.class.getSimpleName());

        return msgCls;
    }

    /** */
    private static class BlockingCommunicationSpi extends LogCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (blkMessage(msg)) {
                try {
                    txLatch.countDown();

                    cutLatch.await(1_000, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            super.sendMessage(node, msg, ackC);
        }

        /** */
        private boolean blkMessage(Message msg) {
            if (msg instanceof GridIoMessage) {
                msg = ((GridIoMessage)msg).message();

                return msg.getClass().getSimpleName().equals(blkMsgCls);
            }

            return false;
        }
    }
}
