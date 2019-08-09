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

package org.apache.ignite.spi.communication.tcp.internal;

import java.io.Serializable;
import java.nio.channels.Channel;
import java.util.UUID;
import org.apache.ignite.spi.communication.CommunicationListener;

/**
 * Extended communication SPI listener to provide {@link Channel} opened events.
 */
public interface CommunicationListenerEx<T extends Serializable> extends CommunicationListener<T> {
    /**
     * @param rmtNodeId Remote node id.
     * @param initMsg Init channel message.
     * @param channel Locally created channel endpoint.
     */
    public default void onChannelOpened(UUID rmtNodeId, T initMsg, Channel channel) {
        // No-op.
    }
}
