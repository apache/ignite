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

package org.apache.ignite.spi.discovery;

import java.io.Serializable;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.jetbrains.annotations.Nullable;

/**
 * Message to send across ring.
 *
 * @see GridDiscoveryManager#sendCustomEvent(DiscoveryCustomMessage)
 */
public interface DiscoverySpiCustomMessage extends Serializable {
    /**
     * Called when custom message has been handled by all nodes.
     *
     * @return Ack message or {@code null} if ack is not required.
     */
    @Nullable public DiscoverySpiCustomMessage ackMessage();

    /**
     * @return {@code True} if message can be modified during listener notification. Changes will be send to next nodes.
     */
    public boolean isMutable();

    /**
     * Called on discovery coordinator node after listener is notified. If returns {@code true}
     * then message is not passed to others nodes, if after this method {@link #ackMessage()} returns non-null ack
     * message, it is sent to all nodes.
     *
     * @return {@code True} if message should not be sent to all nodes.
     *
     * @deprecated Is not used anymore and will be removed at 3.0.
     */
    @Deprecated
    public boolean stopProcess();
}
