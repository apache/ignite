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

package org.apache.ignite.internal.managers.communication;

import java.nio.channels.Channel;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * A message with additional {@link Channel} attibutes which is send on connection established and
 * an appropriate channel is opened.
 */
public class SessionChannelMessage implements Message {
    /** Initial channel message type (value is {@code 175}). */
    public static final short TYPE_CODE = 175;

    /** Channel session unique identifier. */
    @Order(0)
    private IgniteUuid sesId;

    /**
     * Default constructor.
     */
    public SessionChannelMessage() {
        // No-op.
    }

    /**
     * @param sesId Channel session unique identifier.
     */
    public SessionChannelMessage(IgniteUuid sesId) {
        this.sesId = sesId;
    }

    /**
     * @return The unique session id for the channel.
     */
    public IgniteUuid sesId() {
        return sesId;
    }

    /**
     * @param sesId The unique session id for the channel.
     */
    public void sesId(IgniteUuid sesId) {
        this.sesId = sesId;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SessionChannelMessage.class, this);
    }

}
