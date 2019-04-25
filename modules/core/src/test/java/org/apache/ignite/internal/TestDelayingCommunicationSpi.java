/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

/**
 *
 */
public abstract class TestDelayingCommunicationSpi extends TcpCommunicationSpi {
    /** {@inheritDoc} */
    @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
        throws IgniteSpiException {
        try {
            GridIoMessage ioMsg = (GridIoMessage)msg;

            if (delayMessage(ioMsg.message(), ioMsg))
                U.sleep(ThreadLocalRandom.current().nextInt(delayMillis()) + 1);
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new IgniteSpiException(e);
        }

        super.sendMessage(node, msg, ackC);
    }

    /**
     * @param msg Message.
     * @param ioMsg Wrapper message.
     * @return {@code True} if need delay message.
     */
    protected abstract boolean delayMessage(Message msg, GridIoMessage ioMsg);

    /**
     * @return Max delay time.
     */
    protected int delayMillis() {
        return 250;
    }
}
