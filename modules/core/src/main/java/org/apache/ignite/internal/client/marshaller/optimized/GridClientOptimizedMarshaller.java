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

package org.apache.ignite.internal.client.marshaller.optimized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.client.marshaller.GridClientMarshaller;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.processors.rest.client.message.GridClientMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.PluginProvider;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper, that adapts {@link OptimizedMarshaller} to
 * {@link GridClientMarshaller} interface.
 */
public class GridClientOptimizedMarshaller implements GridClientMarshaller {
    /** ID. */
    public static final byte ID = 1;

    /** Optimized marshaller. */
    protected final OptimizedMarshaller opMarsh;

    /**
     * Default constructor.
     */
    public GridClientOptimizedMarshaller() {
        opMarsh = new OptimizedMarshaller();

        opMarsh.setContext(new ClientMarshallerContext());
    }

    /**
     * Constructor.
     *
     * @param plugins Plugins.
     */
    public GridClientOptimizedMarshaller(@Nullable List<PluginProvider> plugins) {
        opMarsh = new OptimizedMarshaller();

        opMarsh.setContext(new ClientMarshallerContext(plugins));
    }

    /**
     * Constructs optimized marshaller with specific parameters.
     *
     * @param requireSer Require serializable flag.
     * @param poolSize Object streams pool size.
     * @throws IOException If an I/O error occurs while writing stream header.
     * @throws IgniteException If this marshaller is not supported on the current JVM.
     * @see OptimizedMarshaller
     */
    public GridClientOptimizedMarshaller(boolean requireSer, int poolSize) throws IOException {
        opMarsh = new OptimizedMarshaller();

        opMarsh.setContext(new ClientMarshallerContext());
        opMarsh.setRequireSerializable(requireSer);
        opMarsh.setPoolSize(poolSize);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer marshal(Object obj, int off) throws IOException {
        try {
            if (!(obj instanceof GridClientMessage))
                throw new IOException("Message serialization of given type is not supported: " +
                    obj.getClass().getName());

            byte[] bytes = U.marshal(opMarsh, obj);

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
            return U.unmarshal(opMarsh, bytes, null);
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /**
     */
    private static class ClientMarshallerContext extends MarshallerContextImpl {
        /** */
        public ClientMarshallerContext() {
            super(null, null);
        }

        /**
         * @param plugins Plugins.
         */
        public ClientMarshallerContext(@Nullable List<PluginProvider> plugins) {
            super(plugins, null);
        }
    }
}
