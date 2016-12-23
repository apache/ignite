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

import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.Collection;
import org.jetbrains.annotations.Nullable;

/**
 * Binary type proxy. Is used to delay or completely avoid metadata lookup.
 */
public class BinaryTypeProxy implements BinaryType {
    /** Binary context. */
    @GridToStringExclude
    private final BinaryContext ctx;

    /** Type ID. */
    private int typeId;

    /** Raw data. */
    private final String clsName;

    /** Target type. */
    @GridToStringExclude
    private volatile BinaryType target;

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param typeId Type ID.
     * @param clsName Class name.
     */
    public BinaryTypeProxy(BinaryContext ctx, int typeId, @Nullable String clsName) {
        this.ctx = ctx;
        this.typeId = typeId;
        this.clsName = clsName;
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return typeId;
    }

    /** {@inheritDoc} */
    @Override public BinaryField field(String fieldName) {
        return ctx.createField(typeId, fieldName);
    }

    /** {@inheritDoc} */
    @Override public String typeName() {
        return target().typeName();
    }

    /** {@inheritDoc} */
    @Override public Collection<String> fieldNames() {
        return target().fieldNames();
    }

    /** {@inheritDoc} */
    @Override public String fieldTypeName(String fieldName) {
        return target().fieldTypeName(fieldName);
    }

    /** {@inheritDoc} */
    @Override public String affinityKeyFieldName() {
        return target().affinityKeyFieldName();
    }

    /** {@inheritDoc} */
    @Override public boolean isEnum() {
        return target().isEnum();
    }

    /**
     * @return Target type.
     */
    private BinaryType target() {
        if (target == null) {
            synchronized (this) {
                if (target == null) {
                    if (typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID && clsName != null)
                        typeId = ctx.typeId(clsName);

                    target = ctx.metadata(typeId);

                    if (target == null)
                        throw new BinaryObjectException("Failed to get binary type details [typeId=" + typeId + ']');
                }
            }
        }

        return target;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryTypeProxy.class, this);
    }
}
