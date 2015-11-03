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

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryField;

/**
 * Implementation of portable field descriptor.
 */
public class BinaryFieldImpl implements BinaryField {
    /** Well-known object schemas. */
    private final PortableSchemaRegistry schemas;

    /** Pre-calculated field ID. */
    private final int fieldId;

    /**
     * Constructor.
     *
     * @param schemas Schemas.
     * @param fieldId Field ID.
     */
    public BinaryFieldImpl(PortableSchemaRegistry schemas, int fieldId) {
        this.schemas = schemas;
        this.fieldId = fieldId;
    }

    /** {@inheritDoc} */
    @Override public boolean exists(BinaryObject obj) {
        BinaryObjectEx obj0 = (BinaryObjectEx)obj;

        return fieldOrder(obj0) != 0;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T value(BinaryObject obj) {
        BinaryObjectEx obj0 = (BinaryObjectEx)obj;

        int order = fieldOrder(obj0);

        return order != 0 ? (T)obj0.fieldByOrder(order) : null;
    }

    /**
     * Get relative field offset.
     *
     * @param obj Object.
     * @return Field offset.
     */
    private int fieldOrder(BinaryObjectEx obj) {
        int schemaId = obj.schemaId();

        PortableSchema schema = schemas.schema(schemaId);

        if (schema == null) {
            schema = obj.createSchema();

            schemas.addSchema(schemaId, schema);
        }

        assert schema != null;

        return schema.order(fieldId);
    }
}
