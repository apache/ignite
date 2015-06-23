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

package org.apache.ignite.marshaller.optimized;

import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * TODO: IGNITE-950
 */
public interface OptimizedFieldsReader {
    /**
     * @param fieldName Field name.
     * @return Byte value.
     * @throws IOException In case of error.
     */
    public byte readByte(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Short value.
     * @throws IOException In case of error.
     */
    public short readShort(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Integer value.
     * @throws IOException In case of error.
     */
    public int readInt(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Long value.
     * @throws IOException In case of error.
     */
    public long readLong(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @throws IOException In case of error.
     * @return Float value.
     */
    public float readFloat(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Double value.
     * @throws IOException In case of error.
     */
    public double readDouble(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Char value.
     * @throws IOException In case of error.
     */
    public char readChar(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Boolean value.
     * @throws IOException In case of error.
     */
    public boolean readBoolean(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return String value.
     * @throws IOException In case of error.
     */
    @Nullable public String readString(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Object.
     * @throws IOException In case of error.
     */
    @Nullable public <T> T readObject(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Byte array.
     * @throws IOException In case of error.
     */
    @Nullable public byte[] readByteArray(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Short array.
     * @throws IOException In case of error.
     */
    @Nullable public short[] readShortArray(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Integer array.
     * @throws IOException In case of error.
     */
    @Nullable public int[] readIntArray(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Long array.
     * @throws IOException In case of error.
     */
    @Nullable public long[] readLongArray(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Float array.
     * @throws IOException In case of error.
     */
    @Nullable public float[] readFloatArray(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Byte array.
     * @throws IOException In case of error.
     */
    @Nullable public double[] readDoubleArray(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Char array.
     * @throws IOException In case of error.
     */
    @Nullable public char[] readCharArray(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Boolean array.
     * @throws IOException In case of error.
     */
    @Nullable public boolean[] readBooleanArray(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return String array.
     * @throws IOException In case of error.
     */
    @Nullable public String[] readStringArray(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Object array.
     * @throws IOException In case of error.
     */
    @Nullable public Object[] readObjectArray(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Collection.
     * @throws IOException In case of error.
     */
    @Nullable public <T> Collection<T> readCollection(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Map.
     * @throws IOException In case of error.
     */
    @Nullable public <K, V> Map<K, V> readMap(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Value.
     * @throws IOException In case of error.
     */
    @Nullable public <T extends Enum<?>> T readEnum(String fieldName) throws IOException;

    /**
     * @param fieldName Field name.
     * @return Value.
     * @throws IOException In case of error.
     */
    @Nullable public <T extends Enum<?>> T[] readEnumArray(String fieldName) throws IOException;
}
