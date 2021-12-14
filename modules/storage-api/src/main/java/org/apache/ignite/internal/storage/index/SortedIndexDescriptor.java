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

package org.apache.ignite.internal.storage.index;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.IndexColumnView;
import org.apache.ignite.configuration.schemas.table.SortedIndexView;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.configuration.SchemaDescriptorConverter;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.schema.definition.ColumnDefinition;

/**
 * Descriptor for creating a Sorted Index Storage.
 *
 * @see SortedIndexStorage
 */
public class SortedIndexDescriptor {
    /**
     * Descriptor of a Sorted Index column (column name and column sort order).
     */
    public static class ColumnDescriptor {
        private final Column column;

        private final boolean asc;

        private final boolean indexedColumn;

        private final boolean nullable;

        ColumnDescriptor(Column column, boolean asc, boolean indexedColumn, boolean nullable) {
            this.column = column;
            this.asc = asc;
            this.indexedColumn = indexedColumn;
            this.nullable = nullable;
        }

        /**
         * Returns a column descriptor.
         */
        public Column column() {
            return column;
        }

        /**
         * Returns {@code true} if this column is sorted in ascending order or {@code false} otherwise.
         */
        public boolean asc() {
            return asc;
        }

        /**
         * Returns {@code true} if this column was explicitly marked as an indexed column or {@code false} if it is a part of a Primary Key
         * appended for uniqueness.
         */
        public boolean indexedColumn() {
            return indexedColumn;
        }

        /**
         * Returns {@code true} if this column can contain null values or {@code false} otherwise.
         */
        public boolean nullable() {
            return nullable;
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }

    private final String name;

    private final List<ColumnDescriptor> columns;

    private final SchemaDescriptor schemaDescriptor;

    /**
     * Creates an Index Descriptor from a given Table Configuration.
     *
     * @param name        index name.
     * @param tableConfig table configuration.
     */
    public SortedIndexDescriptor(String name, TableView tableConfig) {
        this.name = name;

        TableIndexView indexConfig = tableConfig.indices().get(name);

        if (indexConfig == null) {
            throw new StorageException(String.format("Index configuration for \"%s\" could not be found", name));
        }

        if (!(indexConfig instanceof SortedIndexView)) {
            throw new StorageException(String.format(
                    "Index \"%s\" is not configured as a Sorted Index. Actual type: %s",
                    name, indexConfig.type()
            ));
        }

        // extract indexed column configurations from the table configuration
        NamedListView<? extends IndexColumnView> indexColumns = ((SortedIndexView) indexConfig).columns();

        Stream<String> indexColumnNames = indexColumns.namedListKeys().stream();

        // append the primary key to guarantee index key uniqueness
        Stream<String> primaryKeyColumnNames = Arrays.stream(tableConfig.primaryKey().columns());

        List<ColumnView> indexKeyColumnViews = Stream.concat(indexColumnNames, primaryKeyColumnNames)
                .distinct() // remove Primary Key columns if they are already present in the index definition
                .map(columnName -> {
                    ColumnView columnView = tableConfig.columns().get(columnName);

                    assert columnView != null : "Incorrect index column configuration. " + columnName + " column does not exist";

                    return columnView;
                })
                .collect(toList());

        schemaDescriptor = createSchemaDescriptor(indexKeyColumnViews);

        columns = Arrays.stream(schemaDescriptor.keyColumns().columns())
                .sorted(Comparator.comparingInt(Column::columnOrder))
                .map(column -> {
                    IndexColumnView columnView = indexColumns.get(column.name());

                    // if the index config does not contain this column - it's a column from the Primary Key
                    boolean indexedColumn = columnView != null;

                    // PK columns are always sorted in ascending order
                    boolean asc = !indexedColumn || columnView.asc();

                    return new ColumnDescriptor(column, asc, indexedColumn, column.nullable());
                })
                .collect(toUnmodifiableList());
    }

    /**
     * Creates a {@link SchemaDescriptor} from a list of index key columns.
     */
    private static SchemaDescriptor createSchemaDescriptor(List<ColumnView> indexKeyColumnViews) {
        Column[] keyColumns = new Column[indexKeyColumnViews.size()];

        for (int i = 0; i < indexKeyColumnViews.size(); ++i) {
            ColumnView columnView = indexKeyColumnViews.get(i);

            ColumnDefinition columnDefinition = SchemaConfigurationConverter.convert(columnView);

            keyColumns[i] = SchemaDescriptorConverter.convert(i, columnDefinition);
        }

        return new SchemaDescriptor(0, keyColumns, new Column[0]);
    }

    /**
     * Returns this index' name.
     */
    public String name() {
        return name;
    }

    /**
     * Returns the Column Descriptors that comprise a row of this index (indexed columns + primary key columns).
     */
    public List<ColumnDescriptor> indexRowColumns() {
        return columns;
    }

    /**
     * Converts this Descriptor into an equivalent {@link SchemaDescriptor}.
     *
     * <p>The resulting {@code SchemaDescriptor} will have empty {@link SchemaDescriptor#valueColumns()} and its
     * {@link SchemaDescriptor#keyColumns()} will be consistent with the columns returned by {@link #indexRowColumns()}.
     */
    public SchemaDescriptor asSchemaDescriptor() {
        return schemaDescriptor;
    }
}
