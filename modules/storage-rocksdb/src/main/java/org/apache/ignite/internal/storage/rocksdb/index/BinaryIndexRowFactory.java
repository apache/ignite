/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.rocksdb.index;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowFactory;
import org.apache.ignite.internal.storage.index.IndexRowPrefix;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;

/**
 * {@link IndexRowFactory} implementation that uses {@link BinaryRow} as the index keys serialization mechanism.
 */
class BinaryIndexRowFactory implements IndexRowFactory {
    private final SortedIndexDescriptor descriptor;

    BinaryIndexRowFactory(SortedIndexDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public IndexRow createIndexRow(Object[] columnValues, SearchRow primaryKey) {
        if (columnValues.length != descriptor.indexRowColumns().size()) {
            throw new IllegalArgumentException(String.format(
                    "Incorrect number of column values passed. Expected %d, got %d",
                    descriptor.indexRowColumns().size(),
                    columnValues.length
            ));
        }

        RowAssembler rowAssembler = createRowAssembler(columnValues);

        for (Column column : descriptor.asSchemaDescriptor().keyColumns().columns()) {
            Object columnValue = columnValues[column.columnOrder()];

            RowAssembler.writeValue(rowAssembler, column, columnValue);
        }

        return new BinaryIndexRow(rowAssembler.build(), primaryKey);
    }

    @Override
    public IndexRowPrefix createIndexRowPrefix(Object[] prefixColumnValues) {
        if (prefixColumnValues.length > descriptor.indexRowColumns().size()) {
            throw new IllegalArgumentException(String.format(
                    "Incorrect number of column values passed. Expected not more than %d, got %d",
                    descriptor.indexRowColumns().size(),
                    prefixColumnValues.length
            ));
        }

        return () -> prefixColumnValues;
    }

    /**
     * Creates a {@link RowAssembler} that can later be used to serialized the given column mapping.
     */
    private RowAssembler createRowAssembler(Object[] rowColumns) {
        SchemaDescriptor schemaDescriptor = descriptor.asSchemaDescriptor();

        int nonNullVarlenKeyCols = 0;

        for (Column column : schemaDescriptor.keyColumns().columns()) {
            Object columnValue = rowColumns[column.columnOrder()];

            if (!column.type().spec().fixedLength() && columnValue != null) {
                nonNullVarlenKeyCols += 1;
            }
        }

        return new RowAssembler(schemaDescriptor, nonNullVarlenKeyCols, 0);
    }
}
