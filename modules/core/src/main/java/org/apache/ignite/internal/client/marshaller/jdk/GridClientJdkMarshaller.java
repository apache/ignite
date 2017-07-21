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

package org.apache.ignite.internal.client.marshaller.jdk;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.client.marshaller.GridClientMarshaller;
import org.apache.ignite.internal.util.io.GridByteArrayOutputStream;

/**
 * Simple marshaller that utilize JDK serialization features.
 */
public class GridClientJdkMarshaller implements GridClientMarshaller {
    /** ID. */
    public static final byte ID = 2;

    /** {@inheritDoc} */
    @Override public ByteBuffer marshal(Object obj, int off) throws IOException {
        GridByteArrayOutputStream bOut = new GridByteArrayOutputStream();

        ObjectOutput out = new ObjectOutputStream(bOut);

        out.writeObject(obj);

        out.flush();

        ByteBuffer buf = ByteBuffer.allocate(off + bOut.size());

        buf.position(off);

        buf.put(bOut.internalArray(), 0, bOut.size());

        buf.flip();

        return buf;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unmarshal(byte[] bytes) throws IOException {
        ByteArrayInputStream tmp = new ByteArrayInputStream(bytes);

        ObjectInput in = new ObjectInputStream(tmp);

        try {
            return (T)in.readObject();
        }
        catch (ClassNotFoundException e) {
            throw new IOException("Failed to unmarshal target object: " + e.getMessage(), e);
        }
    }
}