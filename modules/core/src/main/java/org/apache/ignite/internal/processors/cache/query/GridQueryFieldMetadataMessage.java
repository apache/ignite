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

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.plugin.extensions.communication.Message;

/** */
public class GridQueryFieldMetadataMessage implements GridQueryFieldMetadata, Message {
    /** */
    @Order(0)
    protected String schemaName;

    /** */
    @Order(1)
    protected String typeName;

    /** */
    @Order(2)
    protected String fieldName;

    /** */
    @Order(3)
    protected String fieldTypeName;

    /** */
    @Order(4)
    protected int precision;

    /** */
    @Order(5)
    protected int scale;

    /** */
    @Order(6)
    protected int nullability;

    /** Blank constructor for external serialization. */
    public GridQueryFieldMetadataMessage() {
        // No-op.
    }

    /** */
    public GridQueryFieldMetadataMessage(
        String schemaName,
        String typeName,
        String fieldName,
        String fieldTypeName,
        int precision,
        int scale,
        int nullability
    ) {
        this.schemaName = schemaName;
        this.typeName = typeName;
        this.fieldName = fieldName;
        this.fieldTypeName = fieldTypeName;
        this.precision = precision;
        this.scale = scale;
        this.nullability = nullability;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 18;
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return schemaName;
    }

    /** */
    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /** {@inheritDoc} */
    @Override public String typeName() {
        return typeName;
    }

    /** */
    public void typeName(String typeName) {
        this.typeName = typeName;
    }

    /** {@inheritDoc} */
    @Override public String fieldName() {
        return fieldName;
    }

    /** */
    public void fieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    /** {@inheritDoc} */
    @Override public String fieldTypeName() {
        return fieldTypeName;
    }

    /** */
    public void fieldTypeName(String fieldTypeName) {
        this.fieldTypeName = fieldTypeName;
    }

    /** {@inheritDoc} */
    @Override public int precision() {
        return precision;
    }

    /** */
    public void precision(int precision) {
        this.precision = precision;
    }

    /** {@inheritDoc} */
    @Override public int scale() {
        return scale;
    }

    /** */
    public void scale(int scale) {
        this.scale = scale;
    }

    /** {@inheritDoc} */
    @Override public int nullability() {
        return nullability;
    }

    /** */
    public void nullability(int nullability) {
        this.nullability = nullability;
    }
}
