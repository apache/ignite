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

package org.apache.ignite.internal.processors.hadoop.v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopSerialization;
import org.jetbrains.annotations.Nullable;

/**
 * The wrapper around external serializer.
 */
public class HadoopSerializationWrapper<T> implements HadoopSerialization {
    /** External serializer - writer. */
    private final Serializer<T> serializer;

    /** External serializer - reader. */
    private final Deserializer<T> deserializer;

    /** Data output for current write operation. */
    private OutputStream currOut;

    /** Data input for current read operation. */
    private InputStream currIn;

    /** Wrapper around current output to provide OutputStream interface. */
    private final OutputStream outStream = new OutputStream() {
        /** {@inheritDoc} */
        @Override public void write(int b) throws IOException {
            currOut.write(b);
        }

        /** {@inheritDoc} */
        @Override public void write(byte[] b, int off, int len) throws IOException {
            currOut.write(b, off, len);
        }
    };

    /** Wrapper around current input to provide InputStream interface. */
    private final InputStream inStream = new InputStream() {
        /** {@inheritDoc} */
        @Override public int read() throws IOException {
            return currIn.read();
        }

        /** {@inheritDoc} */
        @Override public int read(byte[] b, int off, int len) throws IOException {
            return currIn.read(b, off, len);
        }
    };

    /**
     * @param serialization External serializer to wrap.
     * @param cls The class to serialize.
     */
    public HadoopSerializationWrapper(Serialization<T> serialization, Class<T> cls) throws IgniteCheckedException {
        assert cls != null;

        serializer = serialization.getSerializer(cls);
        deserializer = serialization.getDeserializer(cls);

        try {
            serializer.open(outStream);
            deserializer.open(inStream);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void write(DataOutput out, Object obj) throws IgniteCheckedException {
        assert out != null;
        assert obj != null;

        try {
            currOut = (OutputStream)out;

            serializer.serialize((T)obj);

            currOut = null;
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object read(DataInput in, @Nullable Object obj) throws IgniteCheckedException {
        assert in != null;

        try {
            currIn = (InputStream)in;

            T res = deserializer.deserialize((T) obj);

            currIn = null;

            return res;
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteCheckedException {
        try {
            serializer.close();
            deserializer.close();
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }
}