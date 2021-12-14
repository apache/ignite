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
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowDeserializer;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.ColumnDescriptor;

/**
 * {@link IndexRowDeserializer} implementation that uses {@link BinaryRow} infrastructure for deserialization purposes.
 */
class BinaryIndexRowDeserializer implements IndexRowDeserializer {
    private final SortedIndexDescriptor descriptor;

    BinaryIndexRowDeserializer(SortedIndexDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public Object[] indexedColumnValues(IndexRow indexRow) {
        var row = new Row(descriptor.asSchemaDescriptor(), new ByteBufferRow(indexRow.rowBytes()));

        return descriptor.indexRowColumns().stream()
                .filter(ColumnDescriptor::indexedColumn)
                .map(ColumnDescriptor::column)
                .map(column -> column.type().spec().objectValue(row, column.schemaIndex()))
                .toArray();
    }
}
