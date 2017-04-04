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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
    @GridToStringInclude(sensitive = true)
    private int typeId;

    /** Type name. */
    @GridToStringInclude(sensitive = true)
    private String typeName;

    /** Recorded object fields. */
    @GridToStringInclude(sensitive = true)
    private Map<String, BinaryFieldMetadata> fields;

    /** Affinity key field name. */
    @GridToStringInclude(sensitive = true)
    private String affKeyFieldName;

    /** Schemas associated with type. */
    private Collection<BinarySchema> schemas;

    /** Schema IDs registered for this type */
    private Set<Integer> schemaIds;

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
    public BinaryMetadata(int typeId, String typeName, @Nullable Map<String, BinaryFieldMetadata> fields,
        @Nullable String affKeyFieldName, @Nullable Collection<BinarySchema> schemas, boolean isEnum) {
        assert typeName != null;

        this.typeId = typeId;
        this.typeName = typeName;
        this.fields = fields;
        this.affKeyFieldName = affKeyFieldName;
        this.schemas = schemas;

        if (schemas != null) {
            schemaIds = U.newHashSet(schemas.size());

            for (BinarySchema schema : schemas)
                schemaIds.add(schema.schemaId());
        }
        else
            schemaIds = Collections.emptySet();

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
    public Map<String, BinaryFieldMetadata> fieldsMap() {
        return fields != null ? fields : Collections.<String, BinaryFieldMetadata>emptyMap();
    }

    /**
     * @param fieldName Field name.
     * @return Field type name.
     */
    @Nullable public String fieldTypeName(String fieldName) {
        BinaryFieldMetadata meta = fields != null ? fields.get(fieldName) : null;

        return meta != null ? BinaryUtils.fieldTypeName(meta.typeId()) : null;
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
     * @param schemaId Schema ID.
     * @return {@code true} if <b>BinaryMetadata</b> instance has schema with ID specified, {@code false} otherwise.
     */
    public boolean hasSchema(int schemaId) {
        return schemaIds.contains(schemaId);
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
        writeTo(out);
    }

    /**
     * The object implements the writeTo method to save its contents
     * by calling the methods of DataOutput for its primitive values and strings or
     * calling the writeTo method for other objects.
     *
     * @param out the stream to write the object to.
     * @exception IOException Includes any I/O exceptions that may occur.
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeInt(typeId);

        U.writeString(out, typeName);

        if (fields == null)
            out.writeInt(-1);
        else {
            out.writeInt(fields.size());

            for (Map.Entry<String, BinaryFieldMetadata> fieldEntry : fields.entrySet()) {
                U.writeString(out, fieldEntry.getKey());
                fieldEntry.getValue().writeTo(out);
            }
        }

        U.writeString(out, affKeyFieldName);

        if (schemas == null)
            out.writeInt(-1);
        else {
            out.writeInt(schemas.size());

            for (BinarySchema schema : schemas)
                schema.writeTo(out);
        }

        out.writeBoolean(isEnum);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        readFrom(in);
    }

    /**
     * The object implements the readFrom method to restore its
     * contents by calling the methods of DataInput for primitive
     * types and strings or calling readExternal for other objects.  The
     * readFrom method must read the values in the same sequence
     * and with the same types as were written by writeTo.
     *
     * @param in the stream to read data from in order to restore the object.
     * @exception IOException if I/O errors occur.
     */
    public void readFrom(DataInput in) throws IOException {
        typeId = in.readInt();
        typeName = U.readString(in);

        int fieldsSize = in.readInt();

        if (fieldsSize == -1)
            fields = null;
        else {
            fields = new HashMap<>();

            for (int i = 0; i < fieldsSize; i++) {
                String fieldName = U.readString(in);

                BinaryFieldMetadata fieldMeta = new BinaryFieldMetadata();
                fieldMeta.readFrom(in);

                fields.put(fieldName, fieldMeta);
            }
        }

        affKeyFieldName = U.readString(in);

        int schemasSize = in.readInt();

        if (schemasSize == -1)
            schemas = null;
        else {
            schemas = new ArrayList<>();

            schemaIds = U.newHashSet(schemasSize);

            for (int i = 0; i < schemasSize; i++) {
                BinarySchema schema = new BinarySchema();

                schema.readFrom(in);

                schemas.add(schema);

                schemaIds.add(schema.schemaId());
            }
        }

        isEnum = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryMetadata.class, this);
    }
}
