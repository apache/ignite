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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.FieldDescriptor;
import org.apache.ignite.internal.network.serialization.Primitives;

/**
 * {@link ObjectInputStream} specialization used by User Object Serialization.
 */
class UosObjectInputStream extends ObjectInputStream {
    private final DataInputStream input;
    private final ValueReader<Object> valueReader;
    private final DefaultFieldsReaderWriter defaultFieldsReaderWriter;
    private final UnmarshallingContext context;

    private UosGetField currentGet;

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
    public GetField readFields() throws IOException {
        if (currentGet == null) {
            currentGet = new UosGetField(context.descriptorOfObjectCurrentlyReadWithReadObject());
            currentGet.readFields();
        }
        return currentGet;
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
        // no-op
    }

    UosGetField replaceCurrentGetFieldWithNull() {
        UosGetField oldGet = currentGet;
        currentGet = null;
        return oldGet;
    }

    void restoreCurrentGetFieldTo(UosGetField newGet) {
        currentGet = newGet;
    }

    class UosGetField extends GetField {
        private final DataInput input = UosObjectInputStream.this;
        private final ClassDescriptor descriptor;

        private final byte[] primitiveFieldsData;
        private final Object[] objectFieldVals;

        private UosGetField(ClassDescriptor currentObjectDescriptor) {
            this.descriptor = currentObjectDescriptor;

            primitiveFieldsData = new byte[currentObjectDescriptor.primitiveFieldsDataSize()];
            objectFieldVals = new Object[currentObjectDescriptor.objectFieldsCount()];
        }

        /** {@inheritDoc} */
        @Override
        public ObjectStreamClass getObjectStreamClass() {
            return ObjectStreamClass.lookupAny(descriptor.clazz());
        }

        /** {@inheritDoc} */
        @Override
        public boolean defaulted(String name) throws IOException {
            // TODO: IGNITE-15948 - actually take into account whether it's defaulted or not
            return false;
        }

        // TODO: IGNITE-15948 - return default values if the field exists locally but not in the stream being parsed

        /** {@inheritDoc} */
        @Override
        public boolean get(String name, boolean val) throws IOException {
            return Bits.getBoolean(primitiveFieldsData, primitiveFieldDataOffset(name, boolean.class));
        }

        /** {@inheritDoc} */
        @Override
        public byte get(String name, byte val) throws IOException {
            return primitiveFieldsData[primitiveFieldDataOffset(name, byte.class)];
        }

        /** {@inheritDoc} */
        @Override
        public char get(String name, char val) throws IOException {
            return Bits.getChar(primitiveFieldsData, primitiveFieldDataOffset(name, char.class));
        }

        /** {@inheritDoc} */
        @Override
        public short get(String name, short val) throws IOException {
            return Bits.getShort(primitiveFieldsData, primitiveFieldDataOffset(name, short.class));
        }

        /** {@inheritDoc} */
        @Override
        public int get(String name, int val) throws IOException {
            return Bits.getInt(primitiveFieldsData, primitiveFieldDataOffset(name, int.class));
        }

        /** {@inheritDoc} */
        @Override
        public long get(String name, long val) throws IOException {
            return Bits.getLong(primitiveFieldsData, primitiveFieldDataOffset(name, long.class));
        }

        /** {@inheritDoc} */
        @Override
        public float get(String name, float val) throws IOException {
            return Bits.getFloat(primitiveFieldsData, primitiveFieldDataOffset(name, float.class));
        }

        /** {@inheritDoc} */
        @Override
        public double get(String name, double val) throws IOException {
            return Bits.getDouble(primitiveFieldsData, primitiveFieldDataOffset(name, double.class));
        }

        /** {@inheritDoc} */
        @Override
        public Object get(String name, Object val) throws IOException {
            return objectFieldVals[descriptor.objectFieldIndex(name)];
        }

        private int primitiveFieldDataOffset(String fieldName, Class<?> requiredType) {
            return descriptor.primitiveFieldDataOffset(fieldName, requiredType);
        }

        private void readFields() throws IOException {
            int objectFieldIndex = 0;

            for (FieldDescriptor fieldDesc : descriptor.fields()) {
                if (fieldDesc.isPrimitive()) {
                    int offset = descriptor.primitiveFieldDataOffset(fieldDesc.name(), fieldDesc.clazz());
                    int length = Primitives.widthInBytes(fieldDesc.clazz());
                    input.readFully(primitiveFieldsData, offset, length);
                } else {
                    objectFieldVals[objectFieldIndex] = doReadObject();
                    objectFieldIndex++;
                }
            }
        }
    }
}
