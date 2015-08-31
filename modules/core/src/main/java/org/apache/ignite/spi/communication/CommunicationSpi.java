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

package org.apache.ignite.spi.communication;

import java.io.Serializable;
import java.util.Collection;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.Nullable;

/**
 * Communication SPI is responsible for data exchange between nodes.
 * <p>
 * Communication SPI is one of the most important SPI in Ignite. It is used
 * heavily throughout the system and provides means for all data exchanges
 * between nodes, such as internal implementation details and user driven
 * messages.
 * <p>
 * Functionality to this SPI is exposed directly in {@link org.apache.ignite.Ignite} interface:
 * <ul>
 *      <li>{@link org.apache.ignite.IgniteMessaging#send(Object, Object)}
 *      <li>{@link org.apache.ignite.IgniteMessaging#send(Object, Collection)}</li>
 * </ul>
 * <p>
 * Ignite comes with built-in communication SPI implementations:
 * <ul>
 *      <li>{@link org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi}</li>
 * </ul>
 * <b>NOTE:</b> this SPI (i.e. methods in this interface) should never be used directly. SPIs provide
 * internal view on the subsystem and is used internally by Ignite kernal. In rare use cases when
 * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
 * via {@link org.apache.ignite.Ignite#configuration()} method to check its configuration properties or call other non-SPI
 * methods. Note again that calling methods from this interface on the obtained instance can lead
 * to undefined behavior and explicitly not supported.
 */
public interface CommunicationSpi<T extends Serializable> extends IgniteSpi {
    /**
     * Sends given message to destination node. Note that characteristics of the
     * exchange such as durability, guaranteed delivery or error notification is
     * dependant on SPI implementation.
     *
     * @param destNode Destination node.
     * @param msg Message to send.
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown in case of any error during sending the message.
     *      Note that this is not guaranteed that failed communication will result
     *      in thrown exception as this is dependant on SPI implementation.
     */
    public void sendMessage(ClusterNode destNode, T msg) throws IgniteSpiException;

    /**
     * Gets sent messages count.
     *
     * @return Sent messages count.
     */
    public int getSentMessagesCount();

    /**
     * Gets sent bytes count.
     *
     * @return Sent bytes count.
     */
    public long getSentBytesCount();

    /**
     * Gets received messages count.
     *
     * @return Received messages count.
     */
    public int getReceivedMessagesCount();

    /**
     * Gets received bytes count.
     *
     * @return Received bytes count.
     */
    public long getReceivedBytesCount();

    /**
     * Gets outbound messages queue size.
     *
     * @return Outbound messages queue size.
     */
    public int getOutboundMessagesQueueSize();

    /**
     * Resets metrics for this SPI instance.
     */
    public void resetMetrics();

    /**
     * Set communication listener.
     *
     * @param lsnr Listener to set or {@code null} to unset the listener.
     */
    public void setListener(@Nullable CommunicationListener<T> lsnr);
}