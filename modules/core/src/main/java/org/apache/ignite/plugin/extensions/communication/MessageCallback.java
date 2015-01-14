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

package org.apache.ignite.plugin.extensions.communication;

import org.apache.ignite.plugin.*;

import java.nio.*;
import java.util.*;

/**
 * Allows to patch message before sending or after reading.
 */
public interface MessageCallback extends IgniteExtension {
    /**
     * Writes delta for provided node and message type.
     *
     * @param nodeId Node ID.
     * @param msg Message type.
     * @param buf Buffer to write to.
     * @return Whether delta was fully written.
     */
    public boolean onSend(UUID nodeId, Object msg, ByteBuffer buf);

    /**
     * Reads delta for provided node and message type.
     *
     * @param nodeId Node ID.
     * @param msgCls Message type.
     * @param buf Buffer to read from.
     * @return Whether delta was fully read.
     */
    public boolean onReceive(UUID nodeId, Class<?> msgCls, ByteBuffer buf);
}
