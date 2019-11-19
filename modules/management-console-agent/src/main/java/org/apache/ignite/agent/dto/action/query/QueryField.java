/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.dto.action.query;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DTO for query field.
 */
public class QueryField {
    /** Schema name. */
    private String schemaName;

    /** Type name. */
    private String typeName;

    /** Field name. */
    private String fieldName;

    /** Field type name. */
    private String fieldTypeName;

    /**
     * @return Schema name.
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @param schemaName Schema name.
     * @return {@code This} for chaining method calls.
     */
    public QueryField setSchemaName(String schemaName) {
        this.schemaName = schemaName;

        return this;
    }

    /**
     * @return Type name.
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * @param typeName Type name.
     * @return {@code This} for chaining method calls.
     */
    public QueryField setTypeName(String typeName) {
        this.typeName = typeName;

        return this;
    }

    /**
     * @return Field name.
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * @param fieldName Field name.
     * @return {@code This} for chaining method calls.
     */
    public QueryField setFieldName(String fieldName) {
        this.fieldName = fieldName;

        return this;
    }

    /**
     * @return Field type name.
     */
    public String getFieldTypeName() {
        return fieldTypeName;
    }

    /**
     * @param fieldTypeName Field type name.
     * @return {@code This} for chaining method calls.
     */
    public QueryField setFieldTypeName(String fieldTypeName) {
        this.fieldTypeName = fieldTypeName;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryField.class, this);
    }
}
