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

package org.apache.ignite.spi;

import java.util.UUID;

/**
 * Result of joining node validation.
 */
public class IgniteNodeValidationResult {
    /** Offending node ID. */
    private final UUID nodeId;

    /** Error message to be logged locally. */
    private final String msg;

    /** Error message to be sent to joining node. */
    private final String sndMsg;

    /**
     * @param nodeId Offending node ID.
     * @param msg Message logged locally.
     * @param sndMsg Message sent to joining node.
     */
    public IgniteNodeValidationResult(UUID nodeId, String msg, String sndMsg) {
        this.nodeId = nodeId;
        this.msg = msg;
        this.sndMsg = sndMsg;
    }

    /**
     * @return Offending node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Message to be logged locally.
     */
    public String message() {
        return msg;
    }

    /**
     * @return Message to be sent to joining node.
     */
    public String sendMessage() {
        return sndMsg;
    }
}