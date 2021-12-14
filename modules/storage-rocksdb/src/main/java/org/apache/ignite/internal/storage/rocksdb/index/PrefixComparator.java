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

import java.util.Arrays;
import java.util.BitSet;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.index.IndexRowPrefix;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.ColumnDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Class for comparing a {@link BinaryRow} representing an Index Key with a given prefix of index columns.
 */
class PrefixComparator {
    private final SortedIndexDescriptor descriptor;
    private final @Nullable Object[] prefix;

    /**
     * Creates a new prefix comparator.
     *
     * @param descriptor Index Descriptor of the enclosing index.
     * @param prefix Prefix to compare the incoming rows against.
     */
    PrefixComparator(SortedIndexDescriptor descriptor, IndexRowPrefix prefix) {
        assert descriptor.indexRowColumns().size() >= prefix.prefixColumnValues().length;

        this.descriptor = descriptor;
        this.prefix = prefix.prefixColumnValues();
    }

    /**
     * Compares a given row with the configured prefix.
     *
     * @param binaryRow Row to compare.
     * @return the value {@code 0} if the given row starts with the configured prefix;
     *         a value less than {@code 0} if the row's prefix is smaller than the prefix; and
     *         a value greater than {@code 0} if the row's prefix is larger than the prefix.
     */
    int compare(BinaryRow binaryRow) {
        var row = new Row(descriptor.asSchemaDescriptor(), binaryRow);

        for (int i = 0; i < prefix.length; ++i) {
            ColumnDescriptor columnDescriptor = descriptor.indexRowColumns().get(i);

            int compare = compare(columnDescriptor.column(), row, prefix[i]);

            if (compare != 0) {
                return columnDescriptor.asc() ? compare : -compare;
            }
        }

        return 0;
    }

    /**
     * Compares a particular column of a {@code row} with the given value.
     */
    private static int compare(Column column, Row row, @Nullable Object value) {
        boolean nullRow = row.hasNullValue(column.schemaIndex(), column.type().spec());

        if (nullRow && value == null) {
            return 0;
        } else if (nullRow) {
            return -1;
        } else if (value == null) {
            return 1;
        }

        int schemaIndex = column.schemaIndex();

        NativeTypeSpec typeSpec = column.type().spec();

        switch (typeSpec) {
            case INT8:
                return Byte.compare(row.byteValue(schemaIndex), (Byte) value);

            case INT16:
                return Short.compare(row.shortValue(schemaIndex), (Short) value);

            case INT32:
                return Integer.compare(row.intValue(schemaIndex), (Integer) value);

            case INT64:
                return Long.compare(row.longValue(schemaIndex), (Long) value);

            case FLOAT:
                return Float.compare(row.floatValue(schemaIndex), (Float) value);

            case DOUBLE:
                return Double.compare(row.doubleValue(schemaIndex), (Double) value);

            case BYTES:
                return Arrays.compare(row.bytesValue(schemaIndex), (byte[]) value);

            case BITMASK:
                return Arrays.compare(row.bitmaskValue(schemaIndex).toLongArray(), ((BitSet) value).toLongArray());

            // all other types implement Comparable
            case DECIMAL:
            case UUID:
            case STRING:
            case NUMBER:
            case TIMESTAMP:
            case DATE:
            case TIME:
            case DATETIME:
                return ((Comparable) typeSpec.objectValue(row, schemaIndex)).compareTo(value);

            default:
                // should never reach here, this invariant is checked during the index creation
                throw new IllegalStateException(String.format(
                        "Invalid column schema. Column name: %s, column type: %s",
                        column.name(), column.type()
                ));
        }
    }
}
