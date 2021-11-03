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

package org.apache.ignite.internal.schema.mapping;

import java.io.Serializable;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.jetbrains.annotations.NotNull;

/**
 * Column mapping helper.
 */
public class ColumnMapping {
    /** Identity mapper. */
    private static final IdentityMapper IDENTITY_MAPPER = new IdentityMapper();

    /**
     * @return Identity mapper instance.
     */
    public static ColumnMapper identityMapping() {
        return IDENTITY_MAPPER;
    }

    /**
     * @param schema Schema descriptor.
     * @return Column mapper builder.
     */
    public static ColumnMapper createMapper(SchemaDescriptor schema) {
        return new ColumnMapperImpl(schema);
    }

    /**
     * Builds mapper for given schema via merging schema mapper with the provided one. Used for builing columns mapper between arbitraty
     * schema versions with bottom-&gt;top approach.
     *
     * @param mapping Column mapper.
     * @param schema  Target schema.
     * @return Merged column mapper.
     */
    public static ColumnMapper mergeMapping(ColumnMapper mapping, SchemaDescriptor schema) {
        ColumnMapperImpl newMapper = new ColumnMapperImpl(schema);

        ColumnMapper schemaMapper = schema.columnMapping();

        for (int i = 0; i < schema.length(); i++) {
            int idx = schemaMapper.map(i);

            if (idx < 0) {
                newMapper.add(schema.column(i));
            } else {
                newMapper.add0(i, mapping.map(idx), mapping.mappedColumn(idx)); // Remap.
            }
        }

        return newMapper;
    }

    /**
     * Stub.
     */
    private ColumnMapping() {
    }

    /**
     * Identity column mapper.
     */
    private static class IdentityMapper implements ColumnMapper, Serializable {
        /** {@inheritDoc} */
        @Override
        public ColumnMapper add(@NotNull Column col) {
            throw new IllegalStateException("Immutable identity column mapper.");
        }

        /** {@inheritDoc} */
        @Override
        public ColumnMapper add(int from, int to) {
            throw new IllegalStateException("Immutable identity column mapper.");
        }

        /** {@inheritDoc} */
        @Override
        public int map(int idx) {
            return idx;
        }

        /** {@inheritDoc} */
        @Override
        public Column mappedColumn(int idx) {
            return null;
        }
    }
}
