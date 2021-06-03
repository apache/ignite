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

package org.apache.ignite.network;

import org.apache.ignite.network.annotations.MessageGroup;
import org.apache.ignite.network.internal.recovery.message.HandshakeStartMessage;
import org.apache.ignite.network.internal.recovery.message.HandshakeStartResponseMessage;
import org.apache.ignite.network.scalecube.message.ScaleCubeMessage;

/**
 * Message types for the network module.
 */
@MessageGroup(groupName = "NetworkMessages", groupType = 1)
public class NetworkMessageTypes {
    /**
     * Type for {@link ScaleCubeMessage}.
     */
    public static final short SCALE_CUBE_MESSAGE = 1;

    /**
     * Type for {@link HandshakeStartMessage}.
     */
    public static final short HANDSHAKE_START = 2;

    /**
     * Type for {@link HandshakeStartResponseMessage}.
     */
    public static final short HANDSHAKE_START_RESPONSE = 3;
}
