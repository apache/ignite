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

import java.nio.*;
import java.util.*;

/**
 * Communication message writer.
 * <p>
 * Allows to customize the binary format of communication messages.
 */
public interface MessageWriter {
    /**
     * Sets byte buffer to write to.
     *
     * @param buf Byte buffer.
     */
    public void setBuffer(ByteBuffer buf);

    /**
     * Writes message type.
     *
     * @param msgType Message type.
     * @return Whether message type was written.
     */
    public boolean writeMessageType(byte msgType);

    /**
     * Writes field.
     *
     * @param name Field name.
     * @param val Field value.
     * @param type Field type.
     * @return Whether field was fully written.
     */
    public boolean writeField(String name, Object val, MessageFieldType type);

    /**
     * Writes array of objects.
     *
     * @param name Field name.
     * @param arr Array of objects.
     * @param itemType Array component type.
     * @return Whether array was fully written.
     */
    public <T> boolean writeArrayField(String name, T[] arr, MessageFieldType itemType);

    /**
     * Writes collection.
     *
     * @param name Field name.
     * @param col Collection.
     * @param itemType Collection item type.
     * @return Whether value was fully written.
     */
    public <T> boolean writeCollectionField(String name, Collection<T> col, MessageFieldType itemType);

    /**
     * Writes map.
     *
     * @param name Field name.
     * @param map Map.
     * @param keyType Map key type.
     * @param valType Map value type.
     * @return Whether value was fully written.
     */
    public <K, V> boolean writeMapField(String name, Map<K, V> map, MessageFieldType keyType,
        MessageFieldType valType);

    /**
     * @return Whether type for current message is already written.
     */
    public boolean isTypeWritten();

    /**
     * Callback called after message type is written.
     */
    public void onTypeWritten();

    /**
     * Gets current state.
     *
     * @return State.
     */
    public int state();

    /**
     * Increments state.
     */
    public void incrementState();

    /**
     * Callback called before inner message is written.
     */
    public void beforeInnerMessageWrite();

    /**
     * Callback called after inner message is written.
     *
     * @param finished Whether inner message was fully written.
     */
    public void afterInnerMessageWrite(boolean finished);

    /**
     * Resets the state of this writer.
     */
    public void reset();
}
