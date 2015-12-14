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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Binary metadata which is passed over a wire.
 */
public class BinaryMetadata implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Type ID. */
    private int typeId;

    /** Type name. */
    private String typeName;

    /** Recorded object fields. */
    @GridToStringInclude
    private Map<String, Integer> fields;

    /** Affinity key field name. */
    private String affKeyFieldName;

    /** Schemas associated with type. */
    private Collection<BinarySchema> schemas;

    /** Whether this is enum type. */
    private boolean isEnum;

    /**
     * For {@link Externalizable}.
     */
    public BinaryMetadata() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param typeId Type ID.
     * @param typeName Type name.
     * @param fields Fields map.
     * @param affKeyFieldName Affinity key field name.
     * @param schemas Schemas.
     * @param isEnum Enum flag.
     */
    public BinaryMetadata(int typeId, String typeName, @Nullable Map<String, Integer> fields,
        @Nullable String affKeyFieldName, @Nullable Collection<BinarySchema> schemas, boolean isEnum) {
        assert typeName != null;

        this.typeId = typeId;
        this.typeName = typeName;
        this.fields = fields;
        this.affKeyFieldName = affKeyFieldName;
        this.schemas = schemas;
        this.isEnum = isEnum;
    }

    /**
     * @return Type ID.
     */
    public int typeId() {
        return typeId;
    }

    /**
     * @return Type name.
     */
    public String typeName() {
        return typeName;
    }

    /**
     * @return Fields.
     */
    public Collection<String> fields() {
        return fields != null ? fields.keySet() : Collections.<String>emptyList();
    }

    /**
     * @return Fields.
     */
    public Map<String, Integer> fieldsMap() {
        return fields != null ? fields : Collections.<String, Integer>emptyMap();
    }

    /**
     * @param fieldName Field name.
     * @return Field type name.
     */
    @Nullable public String fieldTypeName(String fieldName) {
        Integer typeId = fields != null ? fields.get(fieldName) : null;

        return typeId != null ? BinaryUtils.fieldTypeName(typeId) : null;
    }

    /**
     * @return Affinity key field name.
     */
    @Nullable public String affinityKeyFieldName() {
        return affKeyFieldName;
    }

    /**
     * @return Schemas.
     */
    public Collection<BinarySchema> schemas() {
        return schemas != null ? schemas : Collections.<BinarySchema>emptyList();
    }

    /**
     * @return {@code True} if this is enum type.
     */
    public boolean isEnum() {
        return isEnum;
    }

    /**
     * Wrap metadata into binary type.
     *
     * @param ctx Binary context.
     * @return Binary type.
     */
    public BinaryTypeImpl wrap(BinaryContext ctx) {
        return new BinaryTypeImpl(ctx, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(typeId);
        U.writeString(out, typeName);
        U.writeMap(out, fields);
        U.writeString(out, affKeyFieldName);
        U.writeCollection(out, schemas);
        out.writeBoolean(isEnum);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        typeId = in.readInt();
        typeName = U.readString(in);
        fields = U.readMap(in);
        affKeyFieldName = U.readString(in);
        schemas = U.readCollection(in);
        isEnum = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryMetadata.class, this);
    }
}
