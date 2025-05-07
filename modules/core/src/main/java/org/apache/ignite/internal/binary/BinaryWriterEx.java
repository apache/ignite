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

package org.apache.ignite.internal.binary;

import java.io.InputStream;
import java.io.ObjectOutput;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.jetbrains.annotations.Nullable;

/**
 * Extended writer interface.
 */
public interface BinaryWriterEx extends BinaryWriter, BinaryRawWriter, ObjectOutput {
    /**
     * @param obj Object to write.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    public void writeObjectDetached(@Nullable Object obj) throws BinaryObjectException;

    /**
     * @return Output stream.
     */
    public BinaryOutputStream out();

    /**
     * Cleans resources.
     */
    @Override public void close();

    /**
     * Reserve a room for an integer.
     *
     * @return Position in the stream where value is to be written.
     */
    public int reserveInt();

    /**
     * Write int value at the specific position.
     *
     * @param pos Position.
     * @param val Value.
     * @throws org.apache.ignite.binary.BinaryObjectException If failed.
     */
    public void writeInt(int pos, int val) throws BinaryObjectException;

    /**
     * @return Fail if unregistered flag value.
     */
    public boolean failIfUnregistered();

    /**
     * @param failIfUnregistered Fail if unregistered.
     */
    public void failIfUnregistered(boolean failIfUnregistered);

    /**
     * @param obj Object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void marshal(Object obj) throws BinaryObjectException;

    /**
     * @param typeId Type ID.
     */
    public void typeId(int typeId);

    /**
     * @return Array.
     */
    public byte[] array();

    /**
     * Perform pre-write. Reserves space for header and writes class name if needed.
     *
     * @param clsName Class name (optional).
     */
    public void preWrite(@Nullable String clsName);

    /**
     * Perform post-write. Fills object header.
     *
     * @param userType User type flag.
     * @param registered Whether type is registered.
     */
    public void postWrite(boolean userType, boolean registered);

    /**
     * Perform post-write hash code update if necessary.
     *
     * @param clsName Class name. Always null if class is registered.
     */
    public void postWriteHashCode(@Nullable String clsName);

    /**
     * Pop schema.
     */
    public void popSchema();

    /**
     * Write field ID.
     * @param fieldId Field ID.
     */
    public void writeFieldId(int fieldId);

    /**
     * Create new writer with same context.
     *
     * @param typeId type
     * @return New writer.
     */
    public BinaryWriterEx newWriter(int typeId);

    /**
     * Writes a sub array of bytes.
     * @param b the data to be written
     * @param off       the start offset in the data
     * @param len       the number of bytes that are written
     */
    @Override public void write(byte b[], int off, int len);

    /**
     * @return Schema ID.
     */
    public int schemaId();

    /**
     * @return Current writer's schema.
     */
    public BinarySchema currentSchema();

    /**
     * @return Binary context.
     */
    public BinaryContext context();

    /**
     * @param val Value.
     */
    public void writeBooleanFieldPrimitive(boolean val);

    /**
     * @param val Value.
     */
    public void writeByteFieldPrimitive(byte val);

    /**
     * @param val Value.
     */
    public void writeCharFieldPrimitive(char val);

    /**
     * @param val Value.
     */
    public void writeShortFieldPrimitive(short val);

    /**
     * @param val Value.
     */
    public void writeIntFieldPrimitive(int val);

    /**
     * @param val Value.
     */
    public void writeLongFieldPrimitive(long val);

    /**
     * @param val Value.
     */
    public void writeFloatFieldPrimitive(float val);

    /**
     * @param val Value.
     */
    public void writeDoubleFieldPrimitive(double val);

    /**
     * Write byte array from the InputStream.
     *
     * <p>If {@code limit} > 0 than no more than {@code limit} bytes will be read and written.
     * If {@code limit} == -1 than it will try to read and write all bytes.
     *
     * <p>In any case if actual number of bytes is greater than {@code MAX_ARRAY_SIZE}
     * than exception will be thrown.
     *
     * @param in InputStream.
     * @param limit Max length of data to be read from the stream or -1 if all data should be read.
     * @return Number of bytes written.
     * @throws BinaryObjectException If an I/O error occurs or stream contains more than {@code MAX_ARRAY_SIZE} bytes.
     */
    public int writeByteArray(InputStream in, int limit) throws BinaryObjectException;

    /**
     * @param po Binary object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    public void writeBinaryObject(@Nullable BinaryObjectEx po) throws BinaryObjectException;
}
