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

import java.util.EventListener;
import java.util.UUID;

/**
 * Listener for messages received from remote nodes.
 */
public interface GridMessageListener extends EventListener {
    /**
     * Notification for received messages.
     *
     * @param nodeId ID of node that sent the message. Note that may have already
     *      left topology by the time this message is received.
     * @param msg Message received.
     * @param plc Message policy (pool).
     */
    public void onMessage(UUID nodeId, Object msg, byte plc);
}