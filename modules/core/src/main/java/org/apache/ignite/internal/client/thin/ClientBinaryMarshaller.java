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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryMetadataHandler;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.marshaller.MarshallerContext;

/**
 * Marshals/unmarshals Ignite Binary Objects.
 * <p>
 * Maintains schema registry to allow deserialization from Ignite Binary format to Java type.
 * </p>
 */
class ClientBinaryMarshaller {
    /** Metadata handler. */
    private final BinaryMetadataHandler metaHnd;

    /** Marshaller context. */
    private final MarshallerContext marshCtx;

    /** Re-using marshaller implementation from Ignite core. */
    private GridBinaryMarshaller impl;

    /**
     * Constructor.
     */
    ClientBinaryMarshaller(BinaryMetadataHandler metaHnd, MarshallerContext marshCtx) {
        this.metaHnd = metaHnd;
        this.marshCtx = marshCtx;

        impl = createImpl(null);
    }

    /**
     * Deserializes Ignite binary object from input stream.
     *
     * @return Binary object.
     */
    public <T> T unmarshal(BinaryInputStream in) {
        return impl.unmarshal(in);
    }

    /**
     * Serializes Java object into a byte array.
     */
    public byte[] marshal(Object obj) {
        return impl.marshal(obj, false);
    }

    /**
     * Configure marshaller with custom Ignite Binary Object configuration.
     */
    public void setBinaryConfiguration(BinaryConfiguration binCfg) {
        if (impl.context().configuration().getBinaryConfiguration() != binCfg)
            impl = createImpl(binCfg);
    }

    /**
     * @return The marshaller context.
     */
    public BinaryContext context() {
        return impl.context();
    }

    /** Create new marshaller implementation. */
    private GridBinaryMarshaller createImpl(BinaryConfiguration binCfg) {
        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        if (binCfg == null) {
            binCfg = new BinaryConfiguration();

            binCfg.setCompactFooter(false);
        }

        igniteCfg.setBinaryConfiguration(binCfg);

        BinaryContext ctx = new BinaryContext(metaHnd, igniteCfg, new NullLogger());

        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(marshCtx);

        ctx.configure(marsh, igniteCfg);

        ctx.registerUserTypesSchema();

        return new GridBinaryMarshaller(ctx);
    }
}

