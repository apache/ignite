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

package org.apache.ignite.network.internal;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.message.NetworkMessage;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;

/**
 * Stateful message writer.
 */
public interface MessageWriter {
    /**
     * Sets the byte buffer to write to.
     *
     * @param buf Byte buffer.
     */
    public void setBuffer(ByteBuffer buf);

    /**
     * Sets the type of the message that is currently being written.
     *
     * @param msgCls Message type.
     */
    public void setCurrentWriteClass(Class<? extends NetworkMessage> msgCls);

    /**
     * Writes the header of a message.
     *
     * @param type Message type.
     * @param fieldCnt Fields count.
     * @return {@code true} if successfully. Otherwise returns {@code false}.
     */
    public boolean writeHeader(short type, byte fieldCnt);

    /**
     * Writes a {@code byte} value.
     *
     * @param name Field name.
     * @param val {@code byte} value.
     * @return Whether a value was fully written.
     */
    public boolean writeByte(String name, byte val);

    /**
     * Writes a {@code short} value.
     *
     * @param name Field name.
     * @param val {@code short} value.
     * @return Whether a value was fully written.
     */
    public boolean writeShort(String name, short val);

    /**
     * Writes an {@code int} value.
     *
     * @param name Field name.
     * @param val {@code int} value.
     * @return Whether a value was fully written.
     */
    public boolean writeInt(String name, int val);

    /**
     * Writes a {@code long} value.
     *
     * @param name Field name.
     * @param val {@code long} value.
     * @return Whether a value was fully written.
     */
    public boolean writeLong(String name, long val);

    /**
     * Writes a {@code float} value.
     *
     * @param name Field name.
     * @param val {@code float} value.
     * @return Whether a value was fully written.
     */
    public boolean writeFloat(String name, float val);

    /**
     * Writes a {@code double} value.
     *
     * @param name Field name.
     * @param val {@code double} value.
     * @return Whether a value was fully written.
     */
    public boolean writeDouble(String name, double val);

    /**
     * Writes a {@code char} value.
     *
     * @param name Field name.
     * @param val {@code char} value.
     * @return Whether a value was fully written.
     */
    public boolean writeChar(String name, char val);

    /**
     * Writes a {@code boolean} value.
     *
     * @param name Field name.
     * @param val {@code boolean} value.
     * @return Whether a value was fully written.
     */
    public boolean writeBoolean(String name, boolean val);

    /**
     * Writes a {@code byte} array.
     *
     * @param name Field name.
     * @param val {@code byte} array.
     * @return Whether an array was fully written.
     */
    public boolean writeByteArray(String name, byte[] val);

    /**
     * Writes a {@code byte} array.
     *
     * @param name Field name.
     * @param val {@code byte} array.
     * @param off Offset.
     * @param len Length.
     * @return Whether an array was fully written.
     */
    public boolean writeByteArray(String name, byte[] val, long off, int len);

    /**
     * Writes a {@code short} array.
     *
     * @param name Field name.
     * @param val {@code short} array.
     * @return Whether an array was fully written.
     */
    public boolean writeShortArray(String name, short[] val);

    /**
     * Writes an {@code int} array.
     *
     * @param name Field name.
     * @param val {@code int} array.
     * @return Whether an array was fully written.
     */
    public boolean writeIntArray(String name, int[] val);

    /**
     * Writes a {@code long} array.
     *
     * @param name Field name.
     * @param val {@code long} array.
     * @return Whether an array was fully written.
     */
    public boolean writeLongArray(String name, long[] val);

    /**
     * Writes a {@code long} array.
     *
     * @param name Field name.
     * @param val {@code long} array.
     * @param len Length.
     * @return Whether an array was fully written.
     */
    public boolean writeLongArray(String name, long[] val, int len);

    /**
     * Writes a {@code float} array.
     *
     * @param name Field name.
     * @param val {@code float} array.
     * @return Whether an array was fully written.
     */
    public boolean writeFloatArray(String name, float[] val);

    /**
     * Writes a {@code double} array.
     *
     * @param name Field name.
     * @param val {@code double} array.
     * @return Whether an array was fully written.
     */
    public boolean writeDoubleArray(String name, double[] val);

    /**
     * Writes a {@code char} array.
     *
     * @param name Field name.
     * @param val {@code char} array.
     * @return Whether an array was fully written.
     */
    public boolean writeCharArray(String name, char[] val);

    /**
     * Writes a {@code boolean} array.
     *
     * @param name Field name.
     * @param val {@code boolean} array.
     * @return Whether an array was fully written.
     */
    public boolean writeBooleanArray(String name, boolean[] val);

    /**
     * Writes a {@link String}.
     *
     * @param name Field name.
     * @param val {@link String}.
     * @return Whether a value was fully written.
     */
    public boolean writeString(String name, String val);

    /**
     * Writes a {@link BitSet}.
     *
     * @param name Field name.
     * @param val {@link BitSet}.
     * @return Whether a value was fully written.
     */
    public boolean writeBitSet(String name, BitSet val);

    /**
     * Writes an {@link UUID}.
     *
     * @param name Field name.
     * @param val {@link UUID}.
     * @return Whether a value was fully written.
     */
    public boolean writeUuid(String name, UUID val);

    /**
     * Writes an {@link IgniteUuid}.
     *
     * @param name Field name.
     * @param val {@link IgniteUuid}.
     * @return Whether a value was fully written.
     */
    public boolean writeIgniteUuid(String name, IgniteUuid val);

    /**
     * Writes a nested message.
     *
     * @param name Field name.
     * @param val Message.
     * @return Whether a value was fully written.
     */
    public boolean writeMessage(String name, NetworkMessage val);

    /**
     * Writes an array of objects.
     *
     * @param name Field name.
     * @param arr Array of objects.
     * @param itemType A component type of the array.
     * @return Whether an array was fully written.
     */
    public <T> boolean writeObjectArray(String name, T[] arr, MessageCollectionItemType itemType);

    /**
     * Writes collection.
     *
     * @param name Field name.
     * @param col Collection.
     * @param itemType An item type of the collection.
     * @return Whether a value was fully written.
     */
    public <T> boolean writeCollection(String name, Collection<T> col, MessageCollectionItemType itemType);

    /**
     * Writes a map.
     *
     * @param name Field name.
     * @param map Map.
     * @param keyType The type of the map's key.
     * @param valType The type of the map's value.
     * @return Whether a value was fully written.
     */
    public <K, V> boolean writeMap(String name, Map<K, V> map, MessageCollectionItemType keyType,
        MessageCollectionItemType valType);

    /**
     * @return Whether the header of the current message is already written.
     */
    public boolean isHeaderWritten();

    /**
     * Callback called when the header of the message is written.
     */
    public void onHeaderWritten();

    /**
     * Gets a current message state.
     *
     * @return State.
     */
    public int state();

    /**
     * Increments a state.
     */
    public void incrementState();

    /**
     * Callback called before an inner message is written.
     */
    public void beforeInnerMessageWrite();

    /**
     * Callback called after an inner message is written.
     *
     * @param finished Whether the message was fully written.
     */
    public void afterInnerMessageWrite(boolean finished);

    /**
     * Resets this writer.
     */
    public void reset();
}
