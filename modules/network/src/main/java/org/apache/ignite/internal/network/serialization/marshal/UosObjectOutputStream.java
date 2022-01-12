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

package org.apache.ignite.internal.network.serialization.marshal;

import static org.apache.ignite.internal.network.serialization.marshal.ObjectClass.objectClass;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * {@link ObjectOutputStream} specialization used by User Object Serialization.
 */
class UosObjectOutputStream extends ObjectOutputStream {
    private final DataOutputStream output;
    private final TypedValueWriter valueWriter;
    private final DefaultFieldsReaderWriter defaultFieldsReaderWriter;
    private final MarshallingContext context;

    UosObjectOutputStream(
            DataOutputStream output,
            TypedValueWriter valueWriter,
            DefaultFieldsReaderWriter defaultFieldsReaderWriter,
            MarshallingContext context
    ) throws IOException {
        this.output = output;
        this.valueWriter = valueWriter;
        this.defaultFieldsReaderWriter = defaultFieldsReaderWriter;
        this.context = context;
    }

    /** {@inheritDoc} */
    @Override
    public void write(int val) throws IOException {
        output.write(val);
    }

    /** {@inheritDoc} */
    @Override
    public void write(byte[] buf) throws IOException {
        output.write(buf);
    }

    /** {@inheritDoc} */
    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
        output.write(buf, off, len);
    }

    /** {@inheritDoc} */
    @Override
    public void writeByte(int val) throws IOException {
        output.writeByte(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeShort(int val) throws IOException {
        output.writeShort(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeInt(int val) throws IOException {
        output.writeInt(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeLong(long val) throws IOException {
        output.writeLong(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeFloat(float val) throws IOException {
        output.writeFloat(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeDouble(double val) throws IOException {
        output.writeDouble(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeChar(int val) throws IOException {
        output.writeChar(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeBoolean(boolean val) throws IOException {
        output.writeBoolean(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeBytes(String str) throws IOException {
        output.writeBytes(str);
    }

    /** {@inheritDoc} */
    @Override
    public void writeChars(String str) throws IOException {
        output.writeChars(str);
    }

    /** {@inheritDoc} */
    @Override
    public void writeUTF(String str) throws IOException {
        output.writeUTF(str);
    }

    /** {@inheritDoc} */
    @Override
    protected void writeObjectOverride(Object obj) throws IOException {
        writeObject0(obj);
    }

    private void writeObject0(Object obj) throws IOException {
        try {
            valueWriter.write(obj, objectClass(obj), output, context);
        } catch (MarshalException e) {
            throw new UncheckedMarshalException("Cannot write object", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeUnshared(Object obj) throws IOException {
        // TODO: IGNITE-16257 - implement 'unshared' logic?
        writeObject0(obj);
    }

    /** {@inheritDoc} */
    @Override
    public void defaultWriteObject() throws IOException {
        try {
            defaultFieldsReaderWriter.defaultWriteFields(
                    context.objectCurrentlyWrittenWithWriteObject(),
                    context.descriptorOfObjectCurrentlyWrittenWithWriteObject(),
                    output,
                    context
            );
        } catch (MarshalException e) {
            throw new UncheckedMarshalException("Cannot write fields in a default way", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void useProtocolVersion(int version) {
        // no op
    }

    /** {@inheritDoc} */
    @Override
    public void reset() throws IOException {
        throw new UnsupportedOperationException("The correct way to reset is via MarshallingContext."
                + " Note that it's not valid to call this from writeObject()/readObject() implementation.");
    }

    /** {@inheritDoc} */
    @Override
    public void flush() throws IOException {
        output.flush();
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        flush();
    }
}
