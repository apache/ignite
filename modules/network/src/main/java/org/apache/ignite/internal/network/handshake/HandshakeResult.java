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

package org.apache.ignite.internal.network.handshake;

import java.util.UUID;

/**
 * Handshake result.
 */
public class HandshakeResult {
    /** Handshake action. */
    private final HandshakeAction action;

    /** Remote node consistent id. */
    private final String consistentId;

    /** Remote node launch id. */
    private final UUID launchId;

    /**
     * Constructor for a successful handshake.
     */
    private HandshakeResult(UUID launchId, String consistentId, HandshakeAction action) {
        this.action = action;
        this.consistentId = consistentId;
        this.launchId = launchId;
    }

    public HandshakeAction action() {
        return action;
    }

    public String consistentId() {
        return consistentId;
    }

    public UUID launchId() {
        return launchId;
    }

    public static HandshakeResult fail() {
        return new HandshakeResult(null, null, HandshakeAction.FAIL);
    }

    public static HandshakeResult noOp() {
        return new HandshakeResult(null, null, HandshakeAction.NOOP);
    }

    public static HandshakeResult removeHandler(UUID launchId, String consistentId) {
        return new HandshakeResult(launchId, consistentId, HandshakeAction.REMOVE_HANDLER);
    }
}
