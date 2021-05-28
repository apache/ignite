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

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;

/**
 * Stateful message reader.
 */
public interface MessageReader {
    /**
     * Sets the byte buffer to read from.
     *
     * @param buf Byte buffer.
     */
    public void setBuffer(ByteBuffer buf);

    /**
     * Sets the type of the message that is currently being read.
     *
     * @param msgCls Message type.
     */
    public void setCurrentReadClass(Class<? extends NetworkMessage> msgCls);

    /**
     * Callback that must be invoked by implementations of message serializers before they start decoding the message body.
     *
     * @return {@code True} if a read operation is allowed to proceed, {@code false} otherwise.
     */
    public boolean beforeMessageRead();

    /**
     * Callback that must be invoked by implementations of message serializers after they finished decoding the message body.
     *
     * @param msgCls Class of the message that is finishing read stage.
     * @return {@code True} if a read operation can be proceeded, {@code false} otherwise.
     */
    public boolean afterMessageRead(Class<? extends NetworkMessage> msgCls);

    /**
     * Reads a {@code byte} value.
     *
     * @param name Field name.
     * @return {@code byte} value.
     */
    public byte readByte(String name);

    /**
     * Reads a {@code short} value.
     *
     * @param name Field name.
     * @return {@code short} value.
     */
    public short readShort(String name);

    /**
     * Reads an {@code int} value.
     *
     * @param name Field name.
     * @return {@code int} value.
     */
    public int readInt(String name);

    /**
     * Reads an {@code int} value.
     *
     * @param name Field name.
     * @param dflt A default value if the field is not found.
     * @return {@code int} value.
     */
    public int readInt(String name, int dflt);

    /**
     * Reads a {@code long} value.
     *
     * @param name Field name.
     * @return {@code long} value.
     */
    public long readLong(String name);

    /**
     * Reads a {@code float} value.
     *
     * @param name Field name.
     * @return {@code float} value.
     */
    public float readFloat(String name);

    /**
     * Reads a {@code double} value.
     *
     * @param name Field name.
     * @return {@code double} value.
     */
    public double readDouble(String name);

    /**
     * Reads a {@code char} value.
     *
     * @param name Field name.
     * @return {@code char} value.
     */
    public char readChar(String name);

    /**
     * Reads a {@code boolean} value.
     *
     * @param name Field name.
     * @return {@code boolean} value.
     */
    public boolean readBoolean(String name);

    /**
     * Reads a {@code byte} array.
     *
     * @param name Field name.
     * @return {@code byte} array.
     */
    public byte[] readByteArray(String name);

    /**
     * Reads a {@code short} array.
     *
     * @param name Field name.
     * @return {@code short} array.
     */
    public short[] readShortArray(String name);

    /**
     * Reads an {@code int} array.
     *
     * @param name Field name.
     * @return {@code int} array.
     */
    public int[] readIntArray(String name);

    /**
     * Reads a {@code long} array.
     *
     * @param name Field name.
     * @return {@code long} array.
     */
    public long[] readLongArray(String name);

    /**
     * Reads a {@code float} array.
     *
     * @param name Field name.
     * @return {@code float} array.
     */
    public float[] readFloatArray(String name);

    /**
     * Reads a {@code double} array.
     *
     * @param name Field name.
     * @return {@code double} array.
     */
    public double[] readDoubleArray(String name);

    /**
     * Reads a {@code char} array.
     *
     * @param name Field name.
     * @return {@code char} array.
     */
    public char[] readCharArray(String name);

    /**
     * Reads a {@code boolean} array.
     *
     * @param name Field name.
     * @return {@code boolean} array.
     */
    public boolean[] readBooleanArray(String name);

    /**
     * Reads a {@link String}.
     *
     * @param name Field name.
     * @return {@link String}.
     */
    public String readString(String name);

    /**
     * Reads a {@link BitSet}.
     *
     * @param name Field name.
     * @return {@link BitSet}.
     */
    public BitSet readBitSet(String name);

    /**
     * Reads an {@link UUID}.
     *
     * @param name Field name.
     * @return {@link UUID}.
     */
    public UUID readUuid(String name);

    /**
     * Reads an {@link IgniteUuid}.
     *
     * @param name Field name.
     * @return {@link IgniteUuid}.
     */
    public IgniteUuid readIgniteUuid(String name);

    /**
     * Reads a nested message.
     *
     * @param <T> Type of a message;
     * @param name Field name.
     * @return Message.
     */
    public <T extends NetworkMessage> T readMessage(String name);

    /**
     * Reads an array of objects.
     *
     * @param <T> Type of an array.
     * @param name Field name.
     * @param itemType A component type of the array.
     * @param itemCls A component class of the array.
     * @return Array of objects.
     */
    public <T> T[] readObjectArray(String name, MessageCollectionItemType itemType, Class<T> itemCls);

    /**
     * Reads a collection.
     *
     * @param <C> Type of a collection.
     * @param name Field name.
     * @param itemType An item type of the Collection.
     * @return Collection.
     */
    public <C extends Collection<?>> C readCollection(String name, MessageCollectionItemType itemType);

    /**
     * Reads a map.
     *
     * @param <M> Type of a map.
     * @param name Field name.
     * @param keyType The type of the map's key.
     * @param valType The type of the map's value.
     * @param linked Whether a {@link LinkedHashMap} should be created.
     * @return Map.
     */
    public <M extends Map<?, ?>> M readMap(String name, MessageCollectionItemType keyType,
       MessageCollectionItemType valType, boolean linked);

    /**
     * Tells whether the last invocation of any of the {@code readXXX(...)}
     * methods has fully written the value. {@code False} is returned
     * if there were not enough remaining bytes in a byte buffer.
     *
     * @return Whether the last value was fully read.
     */
    public boolean isLastRead();

    /**
     * Gets a current read state.
     *
     * @return Read state.
     */
    public int state();

    /**
     * Increments a read state.
     */
    public void incrementState();

    /**
     * Callback called before an inner message is read.
     */
    public void beforeInnerMessageRead();

    /**
     * Callback called after an inner message is read.
     *
     * @param finished Whether a message was fully read.
     */
    public void afterInnerMessageRead(boolean finished);

    /**
     * Resets this reader.
     */
    public void reset();
}
