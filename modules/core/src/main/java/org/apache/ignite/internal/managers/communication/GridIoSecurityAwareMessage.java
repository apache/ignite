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

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 *
 */
public class GridIoSecurityAwareMessage extends GridIoMessage {
    /** */
    public static final short TYPE_CODE = 174;

    /** Security subject ID that will be used during message processing on a remote node. */
    @Order(8)
    UUID secSubjId;

    /**
     * Default constructor.
     */
    public GridIoSecurityAwareMessage() {
        // No-op.
    }

    /**
     * @param secSubjId Security subject ID.
     * @param plc Policy.
     * @param topic Communication topic.
     * @param topicOrd Topic ordinal value.
     * @param msg Message.
     * @param ordered Message ordered flag.
     * @param timeout Timeout.
     * @param skipOnTimeout Whether message can be skipped on timeout.
     */
    public GridIoSecurityAwareMessage(
        UUID secSubjId,
        byte plc,
        Object topic,
        int topicOrd,
        Message msg,
        boolean ordered,
        long timeout,
        boolean skipOnTimeout
    ) {
        super(plc, topic, topicOrd, msg, ordered, timeout, skipOnTimeout);

        this.secSubjId = secSubjId;
    }

    /**
     * @return Security subject ID.
     */
    public UUID securitySubjectId() {
        return secSubjId;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
