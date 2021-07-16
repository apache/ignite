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
import org.apache.ignite.internal.schema.SchemaDescriptor;

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
     * @param cols Number of columns.
     * @return Column mapper builder.
     */
    public static ColumnMapperBuilder mapperBuilder(int cols) {
        return new ColumnMapperImpl(cols);
    }

    /**
     * Builds mapper for given schema via merging schema mapper with the provided one.
     * Used for builing columns mapper between arbitraty schema versions with bottom-&gt;top approach.
     *
     * @param mapping Column mapper.
     * @param schema Target schema.
     * @return Merged column mapper.
     */
    public static ColumnMapper mergeMapping(ColumnMapper mapping, SchemaDescriptor schema) {
        if (mapping == identityMapping())
            return schema.columnMapping();

        else if (schema.columnMapping() == identityMapping())
            return mapping;

        ColumnMapperBuilder builder = mapperBuilder(schema.length());

        ColumnMapper schemaMapper = schema.columnMapping();

        for (int i = 0; i < schema.length(); i++) {
            int idx = schemaMapper.map(i);

            if (idx < 0)
                builder.add(i, -1);
            else
                builder.add(i, mapping.map(idx));
        }

        return builder.build();
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
        @Override public int map(int idx) {
            return idx;
        }
    }
}
