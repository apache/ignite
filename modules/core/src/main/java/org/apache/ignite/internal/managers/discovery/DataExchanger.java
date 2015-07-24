package org.apache.ignite.internal.managers.discovery;/*
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

import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public interface DataExchanger<T extends Serializable> {
    /**
     * Gets discovery data object that will be sent to new node
     * during discovery process.
     *
     * @param joinedNodeId ID of new node that joins topology.
     * @return Discovery data object or {@code null} if there is nothing
     *      to send for this component.
     */
    @Nullable public T collectDiscoveryData(UUID joinedNodeId);

    /**
     * @return Topic ID. Must be unique.
     */
    public String topicId();

    /**
     * Receives discovery data object from remote nodes (called
     * on new node during discovery process).
     * @param joiningNodeId Joining node ID.
     * @param rmtNodeId Remote node ID for which data is provided.
     * @param data Discovery data object or {@code null} if nothing was
     */
    public void onDiscoveryDataReceived(UUID joiningNodeId, UUID rmtNodeId, T data);

    /**
     * @return {@code true} if provider should works on last node only.
     */
    public boolean lastNodeOnly();
}
