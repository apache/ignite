/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.client.marshaller.jdk;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.client.marshaller.GridClientMarshaller;
import org.apache.ignite.internal.util.io.GridByteArrayOutputStream;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Simple marshaller that utilize JDK serialization features.
 */
public class GridClientJdkMarshaller implements GridClientMarshaller {
    /** ID. */
    public static final byte ID = 2;

    /** Class name filter. */
    private final IgnitePredicate<String> clsFilter;

    /**
     * Default constructor.
     */
    public GridClientJdkMarshaller() {
        this(null);
    }

    /**
     * @param clsFilter Class filter.
     */
    public GridClientJdkMarshaller(IgnitePredicate<String> clsFilter) {
        this.clsFilter = clsFilter;
    }

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
    @Override public <T> T unmarshal(byte[] bytes) throws IOException {
        ByteArrayInputStream tmp = new ByteArrayInputStream(bytes);

        ObjectInput in = new ClientJdkInputStream(tmp, clsFilter);

        try {
            return (T)in.readObject();
        }
        catch (ClassNotFoundException e) {
            throw new IOException("Failed to unmarshal target object: " + e.getMessage(), e);
        }
    }

    /**
     * Wrapper with class resolving control.
     */
    private static class ClientJdkInputStream extends ObjectInputStream {
        /** Class name filter. */
        private final IgnitePredicate<String> clsFilter;


        /**
         * @param in Input stream.
         * @param clsFilter Class filter.
         */
        public ClientJdkInputStream(InputStream in, IgnitePredicate<String> clsFilter) throws IOException {
            super(in);

            this.clsFilter = clsFilter;
        }

        /** {@inheritDoc} */
        @Override protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            String clsName = desc.getName();

            if (clsFilter != null && !clsFilter.apply(clsName))
                throw new RuntimeException("Deserialization of class " + clsName + " is disallowed.");

            return super.resolveClass(desc);
        }
    }
}