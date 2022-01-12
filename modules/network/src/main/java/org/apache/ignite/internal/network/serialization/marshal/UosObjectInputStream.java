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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * {@link ObjectInputStream} specialization used by User Object Serialization.
 */
class UosObjectInputStream extends ObjectInputStream {
    private final DataInputStream input;
    private final ValueReader<Object> valueReader;
    private final DefaultFieldsReaderWriter defaultFieldsReaderWriter;
    private final UnmarshallingContext context;

    UosObjectInputStream(
            DataInputStream input,
            ValueReader<Object> valueReader,
            DefaultFieldsReaderWriter defaultFieldsReaderWriter,
            UnmarshallingContext context
    ) throws IOException {
        this.input = input;
        this.valueReader = valueReader;
        this.defaultFieldsReaderWriter = defaultFieldsReaderWriter;
        this.context = context;
    }

    /** {@inheritDoc} */
    @Override
    public int read() throws IOException {
        return input.read();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override
    public int read(byte[] buf) throws IOException {
        return input.read(buf);
    }

    /** {@inheritDoc} */
    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        return input.read(buf, off, len);
    }

    /** {@inheritDoc} */
    @Override
    public byte readByte() throws IOException {
        return input.readByte();
    }

    /** {@inheritDoc} */
    @Override
    public short readShort() throws IOException {
        return input.readShort();
    }

    /** {@inheritDoc} */
    @Override
    public int readInt() throws IOException {
        return input.readInt();
    }

    /** {@inheritDoc} */
    @Override
    public long readLong() throws IOException {
        return input.readLong();
    }

    /** {@inheritDoc} */
    @Override
    public float readFloat() throws IOException {
        return input.readFloat();
    }

    /** {@inheritDoc} */
    @Override
    public double readDouble() throws IOException {
        return input.readDouble();
    }

    /** {@inheritDoc} */
    @Override
    public char readChar() throws IOException {
        return input.readChar();
    }

    /** {@inheritDoc} */
    @Override
    public boolean readBoolean() throws IOException {
        return input.readBoolean();
    }

    /** {@inheritDoc} */
    @Override
    public String readUTF() throws IOException {
        return input.readUTF();
    }

    /** {@inheritDoc} */
    @Override
    public int readUnsignedByte() throws IOException {
        return input.readUnsignedByte();
    }

    /** {@inheritDoc} */
    @Override
    public int readUnsignedShort() throws IOException {
        return input.readUnsignedShort();
    }

    /** {@inheritDoc} */
    @Override
    public void readFully(byte[] buf) throws IOException {
        input.readFully(buf);
    }

    /** {@inheritDoc} */
    @Override
    public void readFully(byte[] buf, int off, int len) throws IOException {
        input.readFully(buf, off, len);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override
    public String readLine() throws IOException {
        return input.readLine();
    }

    /** {@inheritDoc} */
    @Override
    protected Object readObjectOverride() throws IOException {
        return doReadObject();
    }

    private Object doReadObject() throws IOException {
        try {
            return valueReader.read(input, context);
        } catch (UnmarshalException e) {
            throw new UncheckedUnmarshalException("Cannot read object", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Object readUnshared() throws IOException {
        // TODO: IGNITE-16257 - implement 'unshared' logic?
        return doReadObject();
    }

    /** {@inheritDoc} */
    @Override
    public void defaultReadObject() throws IOException {
        try {
            defaultFieldsReaderWriter.defaultFillFieldsFrom(
                    input,
                    context.objectCurrentlyReadWithReadObject(),
                    context.descriptorOfObjectCurrentlyReadWithReadObject(),
                    context
            );
        } catch (UnmarshalException e) {
            throw new UncheckedUnmarshalException("Cannot read fields in a default way", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int available() throws IOException {
        return input.available();
    }

    /** {@inheritDoc} */
    @Override
    public int skipBytes(int len) throws IOException {
        return input.skipBytes(len);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
    }
}
