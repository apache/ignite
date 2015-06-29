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

import static org.apache.ignite.marshaller.optimized.OptimizedFieldType.*;

/**
 * TODO: IGNITE-950
 */
class OptimizedMarshalAwareMetaCollector implements OptimizedFieldsWriter {
    /** */
    private OptimizedObjectMetadata meta;

    /**
     * Constructor.
     */
    public OptimizedMarshalAwareMetaCollector() {
        meta = new OptimizedObjectMetadata();
    }

    /** {@inheritDoc} */
    @Override public void writeByte(String fieldName, byte val) throws IOException {
        putFieldToMeta(fieldName, BYTE);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(String fieldName, short val) throws IOException {
        putFieldToMeta(fieldName, SHORT);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(String fieldName, int val) throws IOException {
        putFieldToMeta(fieldName, INT);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(String fieldName, long val) throws IOException {
        putFieldToMeta(fieldName, LONG);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(String fieldName, float val) throws IOException {
        putFieldToMeta(fieldName, FLOAT);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(String fieldName, double val) throws IOException {
        putFieldToMeta(fieldName, DOUBLE);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(String fieldName, char val) throws IOException {
        putFieldToMeta(fieldName, CHAR);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(String fieldName, boolean val) throws IOException {
        putFieldToMeta(fieldName, BOOLEAN);
    }

    /** {@inheritDoc} */
    @Override public void writeString(String fieldName, @Nullable String val) throws IOException {
        putFieldToMeta(fieldName, OTHER);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(String fieldName, @Nullable Object obj) throws IOException {
        putFieldToMeta(fieldName, OTHER);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(String fieldName, @Nullable byte[] val) throws IOException {
        putFieldToMeta(fieldName, OTHER);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(String fieldName, @Nullable short[] val) throws IOException {
        putFieldToMeta(fieldName, OTHER);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(String fieldName, @Nullable int[] val) throws IOException {
        putFieldToMeta(fieldName, OTHER);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(String fieldName, @Nullable long[] val) throws IOException {
        putFieldToMeta(fieldName, OTHER);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(String fieldName, @Nullable float[] val) throws IOException {
        putFieldToMeta(fieldName, OTHER);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(String fieldName, @Nullable double[] val) throws IOException {
        putFieldToMeta(fieldName, OTHER);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(String fieldName, @Nullable char[] val) throws IOException {
        putFieldToMeta(fieldName, OTHER);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(String fieldName, @Nullable boolean[] val) throws IOException {
        putFieldToMeta(fieldName, OTHER);
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(String fieldName, @Nullable String[] val) throws IOException {
        putFieldToMeta(fieldName, OTHER);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(String fieldName, @Nullable Object[] val) throws IOException {
        putFieldToMeta(fieldName, OTHER);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(String fieldName, @Nullable Collection<T> col) throws IOException {
        putFieldToMeta(fieldName, OTHER);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(String fieldName, @Nullable Map<K, V> map) throws IOException {
        putFieldToMeta(fieldName, OTHER);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnum(String fieldName, T val) throws IOException {
        putFieldToMeta(fieldName, OTHER);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnumArray(String fieldName, T[] val) throws IOException {
        putFieldToMeta(fieldName, OTHER);
    }

    /**
     * Returns gather metadata.
     *
     * @return Metadata.
     */
    OptimizedObjectMetadata meta() {
        return meta;
    }

    /**
     * Adds field to the metadata.
     *
     * @param fieldName Field name.
     * @param type Field type.
     */
    private void putFieldToMeta(String fieldName, OptimizedFieldType type) {
        meta.addField(fieldName, type);
    }
}
