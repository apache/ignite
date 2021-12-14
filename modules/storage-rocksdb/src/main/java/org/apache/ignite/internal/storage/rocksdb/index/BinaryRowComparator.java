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

import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingDouble;
import static java.util.Comparator.comparingInt;
import static java.util.Comparator.comparingLong;
import static org.apache.ignite.internal.storage.rocksdb.index.ComparatorUtils.comparingNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.rocksdb.AbstractComparator;
import org.rocksdb.ComparatorOptions;

/**
 * Naive RocksDB comparator implementation that fully de-serializes the passed data and compares the index columns one after the other.
 */
public class BinaryRowComparator extends AbstractComparator {
    /**
     * Actual comparator implementation.
     */
    private final Comparator<ByteBuffer> innerComparator;

    /**
     * Options needed for resource management.
     */
    private final ComparatorOptions options;

    /**
     * Creates a RocksDB comparator for a Sorted Index identified by the given descriptor.
     */
    public BinaryRowComparator(SortedIndexDescriptor descriptor) {
        this(descriptor, new ComparatorOptions());
    }

    /**
     * Internal constructor for capturing the {@code options} parameter for resource management purposes.
     */
    private BinaryRowComparator(SortedIndexDescriptor descriptor, ComparatorOptions options) {
        super(options);

        innerComparator = comparing(
                byteBuffer -> new ByteBufferRow(byteBuffer.order(ByteOrder.LITTLE_ENDIAN)),
                binaryRowComparator(descriptor)
        );

        this.options = options;
    }

    /**
     * Creates a comparator for comparing two {@link BinaryRow}s by converting them into {@link Row}s.
     */
    private static Comparator<BinaryRow> binaryRowComparator(SortedIndexDescriptor descriptor) {
        return comparing(
                binaryRow -> new Row(descriptor.asSchemaDescriptor(), binaryRow),
                rowComparator(descriptor)
        );
    }

    /**
     * Creates a comparator that compares two {@link Row}s by comparing individual columns.
     */
    private static Comparator<Row> rowComparator(SortedIndexDescriptor descriptor) {
        return descriptor.indexRowColumns().stream()
                .map(columnDescriptor -> {
                    Column column = columnDescriptor.column();

                    Comparator<Row> columnComparator = columnComparator(column);

                    if (columnDescriptor.nullable()) {
                        columnComparator = comparingNull(
                                row -> row.hasNullValue(column.schemaIndex(), column.type().spec()) ? null : row,
                                columnComparator
                        );
                    }

                    return columnDescriptor.asc() ? columnComparator : columnComparator.reversed();
                })
                .reduce(Comparator::thenComparing)
                .orElseThrow();
    }

    /**
     * Creates a comparator for comparing table columns.
     */
    private static Comparator<Row> columnComparator(Column column) {
        int schemaIndex = column.schemaIndex();

        NativeTypeSpec typeSpec = column.type().spec();

        switch (typeSpec) {
            case INT8:
                return (row1, row2) -> {
                    byte value1 = row1.byteValue(schemaIndex);
                    byte value2 = row2.byteValue(schemaIndex);

                    return Byte.compare(value1, value2);
                };

            case INT16:
                return (row1, row2) -> {
                    short value1 = row1.shortValue(schemaIndex);
                    short value2 = row2.shortValue(schemaIndex);

                    return Short.compare(value1, value2);
                };

            case INT32:
                return comparingInt(row -> row.intValue(schemaIndex));

            case INT64:
                return comparingLong(row -> row.longValue(schemaIndex));

            case FLOAT:
                return (row1, row2) -> {
                    float value1 = row1.floatValue(schemaIndex);
                    float value2 = row2.floatValue(schemaIndex);

                    return Float.compare(value1, value2);
                };

            case DOUBLE:
                return comparingDouble(row -> row.doubleValue(schemaIndex));

            case BYTES:
                return comparing(row -> row.bytesValue(schemaIndex), Arrays::compare);

            case BITMASK:
                return comparing(row -> row.bitmaskValue(schemaIndex).toLongArray(), Arrays::compare);

            // all other types implement Comparable
            case DECIMAL:
            case UUID:
            case STRING:
            case NUMBER:
            case TIMESTAMP:
            case DATE:
            case TIME:
            case DATETIME:
                return comparing(row -> (Comparable) typeSpec.objectValue(row, schemaIndex));

            default:
                throw new IllegalArgumentException(String.format(
                        "Unsupported column schema for creating a sorted index. Column name: %s, column type: %s",
                        column.name(), column.type()
                ));
        }
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return getClass().getCanonicalName();
    }

    /** {@inheritDoc} */
    @Override
    public int compare(ByteBuffer a, ByteBuffer b) {
        return innerComparator.compare(a, b);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        super.close();

        options.close();
    }
}
