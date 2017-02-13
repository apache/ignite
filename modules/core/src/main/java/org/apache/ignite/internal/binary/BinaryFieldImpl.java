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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryField;

/**
 * Implementation of binary field descriptor.
 */
public class BinaryFieldImpl implements BinaryField {
    /** Type ID. */
    private final int typeId;

    /** Well-known object schemas. */
    @GridToStringExclude
    private final BinarySchemaRegistry schemas;

    /** Field name. */
    private final String fieldName;

    /** Pre-calculated field ID. */
    private final int fieldId;

    /**
     * Constructor.
     *
     * @param schemas Schemas.
     * @param fieldName Field name.
     * @param fieldId Field ID.
     */
    public BinaryFieldImpl(int typeId, BinarySchemaRegistry schemas, String fieldName, int fieldId) {
        assert typeId != 0;
        assert schemas != null;
        assert fieldId != 0;

        this.typeId = typeId;
        this.schemas = schemas;
        this.fieldName = fieldName;
        this.fieldId = fieldId;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return fieldName;
    }

    /**
     * @return Field ID.
     */
    public int fieldId() {
        return fieldId;
    }

    /** {@inheritDoc} */
    @Override public boolean exists(BinaryObject obj) {
        BinaryObjectExImpl obj0 = (BinaryObjectExImpl)obj;

        return fieldOrder(obj0) != BinarySchema.ORDER_NOT_FOUND;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T value(BinaryObject obj) {
        BinaryObjectExImpl obj0 = (BinaryObjectExImpl)obj;

        int order = fieldOrder(obj0);

        return order != BinarySchema.ORDER_NOT_FOUND ? (T)obj0.fieldByOrder(order) : null;
    }

    /**
     * Get relative field offset.
     *
     * @param obj Object.
     * @return Field offset.
     */
    public int fieldOrder(BinaryObjectExImpl obj) {
        if (typeId != obj.typeId()) {
            throw new BinaryObjectException("Failed to get field because type ID of passed object differs" +
                " from type ID this " + BinaryField.class.getSimpleName() + " belongs to [expected=" + typeId +
                ", actual=" + obj.typeId() + ']');
        }

        int schemaId = obj.schemaId();

        BinarySchema schema = schemas.schema(schemaId);

        if (schema == null) {
            schema = obj.createSchema();

            schemas.addSchema(schemaId, schema);
        }

        assert schema != null;

        return schema.order(fieldId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryFieldImpl.class, this);
    }
}
