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

import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.portable.PortableField;
import org.apache.ignite.portable.PortableObject;

/**
 * Implementation of portable field descriptor.
 */
public class PortableFieldImpl implements PortableField {
    /** Well-known object schemas. */
    @GridToStringExclude
    private final PortableSchemaRegistry schemas;

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
    public PortableFieldImpl(PortableSchemaRegistry schemas, String fieldName, int fieldId) {
        assert schemas != null;
        assert fieldName != null;
        assert fieldId != 0;

        this.schemas = schemas;
        this.fieldName = fieldName;
        this.fieldId = fieldId;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return fieldName;
    }

    /** {@inheritDoc} */
    @Override public boolean exists(PortableObject obj) {
        PortableObjectEx obj0 = (PortableObjectEx)obj;

        return fieldOrder(obj0) != PortableSchema.ORDER_NOT_FOUND;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T value(PortableObject obj) {
        PortableObjectEx obj0 = (PortableObjectEx)obj;

        int order = fieldOrder(obj0);

        return order != PortableSchema.ORDER_NOT_FOUND ? (T)obj0.fieldByOrder(order) : null;
    }

    /**
     * Get relative field offset.
     *
     * @param obj Object.
     * @return Field offset.
     */
    private int fieldOrder(PortableObjectEx obj) {
        int schemaId = obj.schemaId();

        PortableSchema schema = schemas.schema(schemaId);

        if (schema == null) {
            schema = obj.createSchema();

            schemas.addSchema(schemaId, schema);
        }

        assert schema != null;

        return schema.order(fieldId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PortableFieldImpl.class, this);
    }
}
