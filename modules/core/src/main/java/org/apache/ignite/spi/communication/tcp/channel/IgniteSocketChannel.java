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

package org.apache.ignite.spi.communication.tcp.channel;

import java.nio.channels.SocketChannel;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.util.nio.GridSelectorNioSession;
import org.apache.ignite.spi.communication.Channel;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Communication TCP/IP socket.
 */
public interface IgniteSocketChannel extends Channel {
    /**
     * @return The underlying java nio {@link SocketChannel} used by the current channel.
     */
    public SocketChannel channel();

    /**
     * Make the channel ready for use.
     */
    public void activate();

    /**
     * @return The type of {@link GridIoPolicy} which defines the processing policy by the communication manager.
     */
    public byte policy();

    /**
     * @param plc The type of {@link GridIoPolicy} to define the processing policy by the communication manager.
     */
    public void policy(byte plc);

    /**
     * @return The communication topic of {@link GridTopic} shows the established channel connection from.
     */
    public Object topic();

    /**
     * @param topic The communication topic of {@link GridTopic} to establish channel connection to.
     */
    public void topic(Object topic);
}
