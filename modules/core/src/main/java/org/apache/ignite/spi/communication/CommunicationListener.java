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
import java.util.EventListener;
import java.util.UUID;
import org.apache.ignite.lang.IgniteRunnable;

/**
 * Listener SPI notifies IO manager with.
 * <p>
 * {@link CommunicationSpi} should ignore very first 4 bytes received from
 * sender node and pass the rest of the message to the listener.
 */
public interface CommunicationListener<T extends Serializable> extends EventListener {
    /**
     * <b>NOTE:</b> {@link CommunicationSpi} should ignore very first 4 bytes received from
     * sender node and pass the rest of the received message to the listener.
     *
     * @param nodeId Node ID.
     * @param msg Message.
     * @param msgC Runnable to call when message processing finished.
     */
    public void onMessage(UUID nodeId, T msg, IgniteRunnable msgC);

    /**
     * Callback invoked when connection with remote node is lost.
     *
     * @param nodeId Node ID.
     */
    public void onDisconnected(UUID nodeId);
}
