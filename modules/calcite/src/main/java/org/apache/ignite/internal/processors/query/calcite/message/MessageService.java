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

package org.apache.ignite.internal.processors.query.calcite.message;

import org.apache.ignite.internal.processors.query.calcite.exec.LifecycleAware;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.network.NetworkMessage;

/**
 *
 */
public interface MessageService extends LifecycleAware {
    /**
     * Sends a message to given node.
     *
     * @param nodeId Node ID.
     * @param msg    Message.
     */
    void send(String nodeId, NetworkMessage msg) throws IgniteInternalCheckedException;

    /**
     * Checks whether a node with given ID is alive.
     *
     * @param nodeId Node ID.
     * @return {@code True} if node is alive.
     */
    boolean alive(String nodeId);

    /**
     * Registers a listener for messages of a given type.
     *
     * @param lsnr  Listener.
     * @param msgId Message id.
     */
    void register(MessageListener lsnr, short msgId);
}
