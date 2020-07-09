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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryMetadataHandler;
import org.apache.ignite.internal.binary.BinaryReaderHandles;
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
     * Unmarshals Ignite binary object from input stream.
     *
     * @param in Input stream.
     * @return Binary object.
     */
    public <T> T unmarshal(BinaryInputStream in) {
        return impl.unmarshal(in);
    }

    /**
     * Deserializes object from input stream.
     *
     * @param in Input stream.
     * @param hnds Object handles.
     */
    public <T> T deserialize(BinaryInputStream in, BinaryReaderHandles hnds) {
        return impl.deserialize(in, null, hnds);
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

        ctx.configure(marsh, binCfg);

        ctx.registerUserTypesSchema();

        return new GridBinaryMarshaller(ctx);
    }
}

