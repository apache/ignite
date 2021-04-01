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

package org.apache.ignite.network.message;

import org.apache.ignite.network.internal.MessageWriter;

/**
 * Message serializer.
 * @param <M> Message type.
 */
public interface MessageSerializer<M extends NetworkMessage> {
    /**
     * Write message to writer.
     * @param message Message.
     * @param writer Message writer.
     * @throws MessageMappingException If failed.
     */
    void writeMessage(M message, MessageWriter writer) throws MessageMappingException;
}
