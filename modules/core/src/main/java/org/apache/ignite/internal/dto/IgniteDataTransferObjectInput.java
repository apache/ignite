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

package org.apache.ignite.internal.dto;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import org.apache.ignite.internal.util.io.GridByteArrayInputStream;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 * Wrapper for object input.
 */
public class IgniteDataTransferObjectInput implements ObjectInput {
    /** */
    private final ObjectInputStream ois;

    /**
     * @param in Target input.
     * @throws IOException If an I/O error occurs.
     */
    public IgniteDataTransferObjectInput(ObjectInput in) throws IOException {
        byte[] buf = U.readByteArray(in);

        /* */
        GridByteArrayInputStream bis = new GridByteArrayInputStream(buf);
        ois = new ObjectInputStream(bis);
    }


    /** {@inheritDoc} */
    @Override public Object readObject() throws ClassNotFoundException, IOException {
        return ois.readObject();
    }

    /** {@inheritDoc} */
    @Override public int read() throws IOException {
        return ois.read();
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] b) throws IOException {
        return ois.read(b);
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] b, int off, int len) throws IOException {
        return ois.read(b, off, len);
    }

    /** {@inheritDoc} */
    @Override public long skip(long n) throws IOException {
        return ois.skip(n);
    }

    /** {@inheritDoc} */
    @Override public int available() throws IOException {
        return ois.available();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        ois.close();
    }

    /** {@inheritDoc} */
    @Override public void readFully(@NotNull byte[] b) throws IOException {
        ois.readFully(b);
    }

    /** {@inheritDoc} */
    @Override public void readFully(@NotNull byte[] b, int off, int len) throws IOException {
        ois.readFully(b, off, len);
    }

    /** {@inheritDoc} */
    @Override public int skipBytes(int n) throws IOException {
        return ois.skipBytes(n);
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() throws IOException {
        return ois.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public byte readByte() throws IOException {
        return ois.readByte();
    }

    /** {@inheritDoc} */
    @Override public int readUnsignedByte() throws IOException {
        return ois.readUnsignedByte();
    }

    /** {@inheritDoc} */
    @Override public short readShort() throws IOException {
        return ois.readShort();
    }

    /** {@inheritDoc} */
    @Override public int readUnsignedShort() throws IOException {
        return ois.readUnsignedShort();
    }

    /** {@inheritDoc} */
    @Override public char readChar() throws IOException {
        return ois.readChar();
    }

    /** {@inheritDoc} */
    @Override public int readInt() throws IOException {
        return ois.readInt();
    }

    /** {@inheritDoc} */
    @Override public long readLong() throws IOException {
        return ois.readLong();
    }

    /** {@inheritDoc} */
    @Override public float readFloat() throws IOException {
        return ois.readFloat();
    }

    /** {@inheritDoc} */
    @Override public double readDouble() throws IOException {
        return ois.readDouble();
    }

    /** {@inheritDoc} */
    @Override public String readLine() throws IOException {
        return ois.readLine();
    }

    /** {@inheritDoc} */
    @NotNull @Override public String readUTF() throws IOException {
        return ois.readUTF();
    }
}
