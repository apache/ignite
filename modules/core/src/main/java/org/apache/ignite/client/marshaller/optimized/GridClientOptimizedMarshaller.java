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

package org.apache.ignite.client.marshaller.optimized;

import org.apache.ignite.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.client.marshaller.*;
import org.apache.ignite.internal.processors.rest.client.message.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Wrapper, that adapts {@link org.apache.ignite.marshaller.optimized.IgniteOptimizedMarshaller} to
 * {@link GridClientMarshaller} interface.
 */
public class GridClientOptimizedMarshaller implements GridClientMarshaller {
    /** ID. */
    public static final byte ID = 1;

    /** Optimized marshaller. */
    private final IgniteOptimizedMarshaller opMarsh;

    /**
     * Default constructor.
     */
    public GridClientOptimizedMarshaller() {
        opMarsh = new IgniteOptimizedMarshaller();
    }

    /**
     * Constructs optimized marshaller with specific parameters.
     *
     * @param requireSer Flag to enforce {@link Serializable} interface or not. If {@code true},
     *      then objects will be required to implement {@link Serializable} in order to be
     *      marshalled, if {@code false}, then such requirement will be relaxed.
     * @param clsNames User preregistered class names.
     * @param clsNamesPath Path to a file with user preregistered class names.
     * @param poolSize Object streams pool size.
     * @throws IOException If an I/O error occurs while writing stream header.
     * @throws IgniteException If this marshaller is not supported on the current JVM.
     * @see org.apache.ignite.marshaller.optimized.IgniteOptimizedMarshaller
     */
    public GridClientOptimizedMarshaller(boolean requireSer, List<String> clsNames, String clsNamesPath, int poolSize)
        throws IOException {
        try {
            opMarsh = new IgniteOptimizedMarshaller(requireSer, clsNames, clsNamesPath, poolSize);
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer marshal(Object obj, int off) throws IOException {
        try {
            if (!(obj instanceof GridClientMessage))
                throw new IOException("Message serialization of given type is not supported: " +
                    obj.getClass().getName());

            byte[] bytes = opMarsh.marshal(obj);

            ByteBuffer buf = ByteBuffer.allocate(off + bytes.length);

            buf.position(off);

            buf.put(bytes);

            buf.flip();

            return buf;
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(byte[] bytes) throws IOException {
        try {
            return opMarsh.unmarshal(bytes, null);
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }
}
