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

package org.apache.ignite.internal.binary;

import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryType;

import java.util.Collection;

import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Binary type implementation.
 */
public class BinaryTypeImpl implements BinaryType {
    /** Binary context. */
    @GridToStringExclude
    private final BinaryContext ctx;

    /** Type metadata. */
    private volatile BinaryMetadata meta;

    /** Type ID. */
    private final int typeId;

    /**
     * Constructor.
     *
     * @param typeId Type ID.
     * @param ctx Binary context.
     * @param meta Type  metadata.
     */
    public BinaryTypeImpl(int typeId, BinaryContext ctx, BinaryMetadata meta) {
        assert ctx != null;

        this.ctx = ctx;
        this.meta = meta;
        this.typeId = typeId;
    }

    /** {@inheritDoc} */
    @Override public String typeName() {
        return metadata().typeName();
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return typeId;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> fieldNames() {
        return metadata().fields();
    }

    /** {@inheritDoc} */
    @Override public String fieldTypeName(String fieldName) {
        return metadata().fieldTypeName(fieldName);
    }

    /** {@inheritDoc} */
    @Override public BinaryFieldImpl field(String fieldName) {
        return ctx.createField(typeId, fieldName);
    }

    /** {@inheritDoc} */
    @Override public String affinityKeyFieldName() {
        return metadata().affinityKeyFieldName();
    }

    /** {@inheritDoc} */
    @Override public boolean isEnum() {
        return metadata().isEnum();
    }

    /**
     * @return Context.
     */
    public BinaryContext context() {
        return ctx;
    }

    /**
     * @return Metadata.
     */
    public BinaryMetadata metadata() {
        loadMetadata();

        if (meta == null)
            throw new IgniteException("No binary metadata available for type ID: " + typeId);

        return meta;
    }

    /**
     * Load metadata if need.
     */
    private void loadMetadata() {
        if (meta == null) {
            synchronized (this) {
                if (meta == null)
                    meta = ctx.binaryMetadata(typeId);
            }
        }
    }

    /**
     * @return {@code True} if metadata available.
     */
    public boolean isMetadataAvailable() {
        loadMetadata();

        return meta != null;
    }


    /** {@inheritDoc} */
    public String toString() {
        return S.toString(BinaryTypeImpl.class, this);
    }
}
