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

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.BinaryClassDescriptor;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilders;
import org.apache.ignite.internal.binary.streams.BinaryStreams;

/**
 * Thin client implementation of {@link IgniteBinary}.
 */
public class ClientBinary implements IgniteBinary {
    /** Marshaller. */
    private final ClientBinaryMarshaller marsh;

    /**
     * Constructor.
     */
    ClientBinary(ClientBinaryMarshaller marsh) {
        this.marsh = marsh;
    }

    /** {@inheritDoc} */
    @Override public int typeId(String typeName) {
        return binaryContext().typeId(typeName);
    }

    /** {@inheritDoc} */
    @Override public <T> T toBinary(Object obj) {
        if (obj == null)
            return null;

        if (BinaryUtils.isBinaryType(obj.getClass()))
            return (T)obj;

        byte[] objBytes = marsh.marshal(obj);

        return (T)marsh.unmarshal(BinaryStreams.createHeapInputStream(objBytes));
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder builder(String typeName) {
        if (typeName == null || typeName.isEmpty())
            throw new IllegalArgumentException("typeName");

        return BinaryObjectBuilders.createBuilder(binaryContext(), typeName);
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder builder(BinaryObject binaryObj) {
        if (binaryObj == null)
            throw new NullPointerException("binaryObj");

        return BinaryObjectBuilders.toBuilder(binaryObj);
    }

    /** {@inheritDoc} */
    @Override public BinaryType type(Class<?> cls) throws BinaryObjectException {
        if (cls == null)
            throw new NullPointerException("cls");

        return type(cls.getName());
    }

    /** {@inheritDoc} */
    @Override public BinaryType type(String typeName) {
        if (typeName == null || typeName.isEmpty())
            throw new IllegalArgumentException("typeName");

        int typeId = binaryContext().typeId(typeName);

        return type(typeId);
    }

    /** {@inheritDoc} */
    @Override public BinaryType type(int typeId) {
        return binaryContext().metadata(typeId);
    }

    /** {@inheritDoc} */
    @Override public Collection<BinaryType> types() {
        return binaryContext().metadata();
    }

    /** {@inheritDoc} */
    @Override public BinaryObject buildEnum(String typeName, int ord) {
        if (typeName == null || typeName.isEmpty())
            throw new IllegalArgumentException("typeName");

        BinaryContext ctx = binaryContext();

        int typeId = ctx.typeId(typeName);

        return new BinaryEnumObjectImpl(ctx, typeId, null, ord);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject buildEnum(String typeName, String name) {
        if (typeName == null || typeName.isEmpty())
            throw new IllegalArgumentException("typeName");

        if (name == null || name.isEmpty())
            throw new IllegalArgumentException("name");

        BinaryContext ctx = binaryContext();

        int typeId = ctx.typeId(typeName);

        BinaryMetadata metadata = ctx.metadata0(typeId);

        if (metadata == null)
            throw new BinaryObjectException(
                String.format("Failed to get metadata for type [typeId=%s, typeName='%s']", typeId, typeName)
            );

        Integer ordinal = metadata.getEnumOrdinalByName(name);

        if (ordinal == null)
            throw new BinaryObjectException(String.format(
                "Failed to resolve enum ordinal by name [typeId=%s, typeName='%s', name='%s']",
                typeId,
                typeName,
                name
            ));

        return new BinaryEnumObjectImpl(ctx, typeId, null, ordinal);
    }

    /** {@inheritDoc} */
    @Override public BinaryType registerEnum(String typeName, Map<String, Integer> vals) {
        if (typeName == null || typeName.isEmpty())
            throw new IllegalArgumentException("typeName");

        BinaryContext ctx = binaryContext();

        int typeId = ctx.typeId(typeName);

        ctx.updateMetadata(typeId, new BinaryMetadata(typeId, typeName, null, null, null, true, vals), false);

        return ctx.metadata(typeId);
    }

    /** {@inheritDoc} */
    @Override public BinaryType registerClass(Class<?> cls) throws BinaryObjectException {
        BinaryClassDescriptor clsDesc = binaryContext().registerClass(cls, true, false);

        return binaryContext().metadata(clsDesc.typeId());
    }

    /** @return Binary context. */
    public BinaryContext binaryContext() {
        return marsh.context();
    }
}
