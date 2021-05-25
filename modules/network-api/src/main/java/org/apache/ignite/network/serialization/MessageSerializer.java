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

package org.apache.ignite.network.serialization;

import org.apache.ignite.network.NetworkMessage;

/**
 * Message serializer.
 * @param <M> Message type.
 */
public interface MessageSerializer<M extends NetworkMessage> {
    /**
     * Writes a message to the writer.
     * @param message Message.
     * @param writer Message writer.
     * @return {@code true } if the message was completely written.
     * @throws MessageMappingException If failed.
     */
    boolean writeMessage(M message, MessageWriter writer) throws MessageMappingException;
}
