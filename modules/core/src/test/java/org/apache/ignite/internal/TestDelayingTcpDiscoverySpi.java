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

package org.apache.ignite.internal;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;

import static org.apache.ignite.testframework.junits.GridAbstractTest.LOCAL_IP_FINDER;

/**
 *
 */
public abstract class TestDelayingTcpDiscoverySpi extends TcpDiscoverySpi {
    /**
     * Default constructor.
     */
    protected TestDelayingTcpDiscoverySpi() {
        setIpFinder(LOCAL_IP_FINDER);
    }

    /** {@inheritDoc} */
    @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
        long timeout) throws IOException {
        holdMessageIfNeeded(msg);

        super.writeToSocket(msg, sock, res, timeout);
    }

    /** {@inheritDoc} */
    @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data,
        long timeout) throws IOException {
        holdMessageIfNeeded(msg);

        super.writeToSocket(sock, msg, data, timeout);
    }

    /** {@inheritDoc} */
    @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
        long timeout) throws IOException, IgniteCheckedException {
        holdMessageIfNeeded(msg);

        super.writeToSocket(sock, out, msg, timeout);
    }

    /**
     * @param msg Message.
     * @return {@code True} if need delay message.
     */
    protected abstract boolean delayMessage(TcpDiscoveryAbstractMessage msg);

    /**
     * @return Delay time.
     */
    protected int delayMillis() {
        return 1000;
    }

    /**
     * @param msg Message.
     */
    protected void holdMessageIfNeeded(TcpDiscoveryAbstractMessage msg) {
        if (delayMessage(msg)) {
            try {
                U.sleep(delayMillis());
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteSpiException(e);
            }
        }
    }
}
