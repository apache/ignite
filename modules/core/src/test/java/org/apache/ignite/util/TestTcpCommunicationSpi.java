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

package org.apache.ignite.util;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

/**
 * TcpCommunicationSpi with additional features needed for tests.
 */
public class TestTcpCommunicationSpi extends TcpCommunicationSpi {
    /** */
    private volatile boolean stopped;

    /** */
    private Class ignoreMsg;

    /** {@inheritDoc} */
    @Override public void sendMessage(final ClusterNode node, final Message msg,
        IgniteInClosure<IgniteException> ackClosure) throws IgniteSpiException {
        if (stopped)
            return;

        if (ignoreMsg != null && ((GridIoMessage)msg).message().getClass().equals(ignoreMsg))
            return;

        super.sendMessage(node, msg, ackClosure);
    }

    /**
     *
     */
    public void stop() {
        stopped = true;
    }

    /**
     *
     */
    public void stop(Class ignoreMsg) {
        this.ignoreMsg = ignoreMsg;
    }

    /**
     * Stop SPI, messages will not send anymore.
     */
    public static void stop(Ignite ignite) {
        ((TestTcpCommunicationSpi)ignite.configuration().getCommunicationSpi()).stop();
    }

    /**
     * Skip messages will not send anymore.
     */
    public static void skipMsgType(Ignite ignite, Class clazz) {
        ((TestTcpCommunicationSpi)ignite.configuration().getCommunicationSpi()).stop(clazz);
    }
}