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

package org.apache.ignite.internal.portable;

import org.apache.ignite.binary.BinaryType;

import java.util.Collection;

/**
 * Binary type implementation.
 */
public class BinaryTypeImpl implements BinaryType {
    /** Portable context. */
    private final PortableContext ctx;

    /** Type metadata. */
    private final BinaryMetadata meta;

    /**
     * Constructor.
     *
     * @param ctx Portable context.
     * @param meta Type  metadata.
     */
    public BinaryTypeImpl(PortableContext ctx, BinaryMetadata meta) {
        this.ctx = ctx;
        this.meta = meta;
    }

    /** {@inheritDoc} */
    @Override public String typeName() {
        return meta.typeName();
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return meta.typeId();
    }

    /** {@inheritDoc} */
    @Override public Collection<String> fieldNames() {
        return meta.fields();
    }

    /** {@inheritDoc} */
    @Override public String fieldTypeName(String fieldName) {
        return meta.fieldTypeName(fieldName);
    }

    /** {@inheritDoc} */
    @Override public BinaryFieldImpl field(String fieldName) {
        return ctx.createField(meta.typeId(), fieldName);
    }

    /** {@inheritDoc} */
    @Override public String affinityKeyFieldName() {
        return meta.affinityKeyFieldName();
    }

    /** {@inheritDoc} */
    @Override public boolean isEnum() {
        return meta.isEnum();
    }

    /**
     * @return Context.
     */
    public PortableContext context() {
        return ctx;
    }

    /**
     * @return Metadata.
     */
    public BinaryMetadata metadata() {
        return meta;
    }
}
