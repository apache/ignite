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

package org.apache.ignite.internal.schema.registry;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.schema.SchemaDescriptor;

/**
 * Adapter for row of older schema.
 */
class UpgradingRowAdapter extends Row {
    /** Column mapper. */
    private final ColumnMapper mapping;

    /**
     * @param schema Schema descriptor of new version.
     * @param row Row.
     * @param mapper Column mapper.
     */
    UpgradingRowAdapter(SchemaDescriptor schema, BinaryRow row, ColumnMapper mapper) {
        super(schema, row);

        this.mapping = mapper;
    }

    /** {@inheritDoc} */
    @Override protected long findColumn(int colIdx, NativeTypeSpec type) throws InvalidTypeException {
        int mapIdx = mapping.map(colIdx);

        return (mapIdx < 0) ? Long.MIN_VALUE : super.findColumn(mapIdx, type);
    }
}
