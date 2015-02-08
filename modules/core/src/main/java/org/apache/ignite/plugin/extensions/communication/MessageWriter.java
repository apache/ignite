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

import org.apache.ignite.lang.*;

import java.nio.*;
import java.util.*;

/**
 * TODO
 */
public interface MessageWriter {
    public void setBuffer(ByteBuffer buf);

    public boolean writeByte(String name, byte val);

    public boolean writeShort(String name, short val);

    public boolean writeInt(String name, int val);

    public boolean writeLong(String name, long val);

    public boolean writeFloat(String name, float val);

    public boolean writeDouble(String name, double val);

    public boolean writeChar(String name, char val);

    public boolean writeBoolean(String name, boolean val);

    public boolean writeByteArray(String name, byte[] val);

    public boolean writeShortArray(String name, short[] val);

    public boolean writeIntArray(String name, int[] val);

    public boolean writeLongArray(String name, long[] val);

    public boolean writeFloatArray(String name, float[] val);

    public boolean writeDoubleArray(String name, double[] val);

    public boolean writeCharArray(String name, char[] val);

    public boolean writeBooleanArray(String name, boolean[] val);

    public boolean writeString(String name, String val);

    public boolean writeBitSet(String name, BitSet val);

    public boolean writeUuid(String name, UUID val);

    public boolean writeIgniteUuid(String name, IgniteUuid val);

    public boolean writeEnum(String name, Enum<?> val);

    public boolean writeMessage(String name, MessageAdapter val);

    public <T> boolean writeObjectArray(String name, T[] arr, Class<T> itemCls);

    public <T> boolean writeCollection(String name, Collection<T> col, Class<T> itemCls);

    public <K, V> boolean writeMap(String name, Map<K, V> map, Class<K> keyCls, Class<V> valCls);
}
