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

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Base class for all communication messages.
 */
public interface Message extends Serializable {
    /**
     * Writes this message to provided byte buffer.
     *
     * @param buf Byte buffer.
     * @param writer Writer.
     * @return Whether message was fully written.
     */
    public boolean writeTo(ByteBuffer buf, MessageWriter writer);

    /**
     * Reads this message from provided byte buffer.
     *
     * @param buf Byte buffer.
     * @param reader Reader.
     * @return Whether message was fully read.
     */
    public boolean readFrom(ByteBuffer buf, MessageReader reader);

    /**
     * Gets message type.
     *
     * @return Message type.
     */
    public byte directType();

    /**
     * Gets fields count.
     *
     * @return Fields count.
     */
    public byte fieldsCount();
}