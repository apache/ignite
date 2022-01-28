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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.configuration.ConfigurationTestUtils.fixConfiguration;
import static org.apache.ignite.internal.schema.SchemaTestUtils.generateRandomValue;
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.schema.SchemaBuilders.column;
import static org.apache.ignite.schema.SchemaBuilders.tableBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.configuration.schemas.store.DataRegionConfiguration;
import org.apache.ignite.configuration.schemas.store.RocksDbDataRegionChange;
import org.apache.ignite.configuration.schemas.store.RocksDbDataRegionConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.DataRegion;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowPrefix;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.ColumnDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.testframework.VariableSource;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.schema.definition.builder.SortedIndexDefinitionBuilder;
import org.apache.ignite.schema.definition.builder.SortedIndexDefinitionBuilder.SortedIndexColumnBuilder;
import org.apache.ignite.schema.definition.index.ColumnarIndexDefinition;
import org.apache.ignite.schema.definition.index.HashIndexDefinition;
import org.apache.ignite.schema.definition.index.SortedIndexDefinition;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;

/**
 * Test class for the {@link RocksDbSortedIndexStorage}.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class RocksDbSortedIndexStorageTest {
    private static final IgniteLogger log = IgniteLogger.forClass(RocksDbSortedIndexStorageTest.class);

    /**
     * Definitions of all supported column types.
     */
    private static final List<ColumnDefinition> ALL_TYPES_COLUMN_DEFINITIONS = allTypesColumnDefinitions();

    private Random random;

    @InjectConfiguration(polymorphicExtensions = {
            HashIndexConfigurationSchema.class,
            SortedIndexConfigurationSchema.class
    })
    private TableConfiguration tableCfg;

    /**
     * Table Storage for creating indices.
     */
    private TableStorage tableStorage;

    /**
     * List of resources that need to be closed at the end of each test.
     */
    private final List<AutoCloseable> resources = new ArrayList<>();

    @BeforeEach
    void setUp(
            @WorkDirectory Path workDir,
            @InjectConfiguration(polymorphicExtensions = RocksDbDataRegionConfigurationSchema.class) DataRegionConfiguration dataRegionCfg
    ) {
        long seed = System.currentTimeMillis();

        log.info("Using random seed: " + seed);

        random = new Random(seed);

        createTestConfiguration(dataRegionCfg);

        dataRegionCfg = fixConfiguration(dataRegionCfg);

        var engine = new RocksDbStorageEngine();

        engine.start();

        resources.add(engine::stop);

        DataRegion dataRegion = engine.createDataRegion(dataRegionCfg);

        dataRegion.start();

        resources.add(() -> {
            dataRegion.beforeNodeStop();
            dataRegion.stop();
        });

        tableStorage = engine.createTable(workDir, tableCfg, dataRegion);

        tableStorage.start();

        resources.add(tableStorage::stop);
    }

    /**
     * Configures a test table with columns of all supported types.
     */
    private void createTestConfiguration(DataRegionConfiguration dataRegionCfg) {
        CompletableFuture<Void> dataRegionChangeFuture = dataRegionCfg
                .change(cfg -> cfg.convert(RocksDbDataRegionChange.class).changeSize(16 * 1024).changeWriteBufferSize(16 * 1024));

        assertThat(dataRegionChangeFuture, willBe(nullValue(Void.class)));

        TableDefinition tableDefinition = tableBuilder("test", "foo")
                .columns(ALL_TYPES_COLUMN_DEFINITIONS.toArray(new ColumnDefinition[0]))
                .withPrimaryKey(ALL_TYPES_COLUMN_DEFINITIONS.get(0).name())
                .build();

        CompletableFuture<Void> createTableFuture = tableCfg.change(cfg -> convert(tableDefinition, cfg));

        assertThat(createTableFuture, willBe(nullValue(Void.class)));
    }

    private static List<ColumnDefinition> allTypesColumnDefinitions() {
        Stream<ColumnType> allColumnTypes = Stream.of(
                ColumnType.INT8,
                ColumnType.INT16,
                ColumnType.INT32,
                ColumnType.INT64,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.UUID,
                ColumnType.DATE,
                ColumnType.bitmaskOf(32),
                ColumnType.string(),
                ColumnType.blobOf(),
                ColumnType.numberOf(),
                ColumnType.decimalOf(),
                ColumnType.time(),
                ColumnType.datetime(),
                ColumnType.timestamp()
        );

        return allColumnTypes
                .map(type -> column(type.typeSpec().name(), type).asNullable(true).build())
                .collect(toUnmodifiableList());
    }

    @AfterEach
    void tearDown() throws Exception {
        Collections.reverse(resources);

        IgniteUtils.closeAll(resources);
    }

    /**
     * Tests that columns of all types are correctly serialized and deserialized.
     */
    @Test
    void testRowSerialization() {
        SortedIndexStorage indexStorage = createIndex(ALL_TYPES_COLUMN_DEFINITIONS);

        Object[] columns = indexStorage.indexDescriptor().indexRowColumns().stream()
                .map(ColumnDescriptor::column)
                .map(column -> generateRandomValue(random, column.type()))
                .toArray();

        IndexRow row = indexStorage.indexRowFactory().createIndexRow(columns, new ByteArraySearchRow(new byte[0]));

        Object[] actual = indexStorage.indexRowDeserializer().indexedColumnValues(row);

        assertThat(actual, is(equalTo(columns)));
    }

    /**
     * Tests the Put-Get-Remove case when an index is created using a single column.
     */
    @ParameterizedTest
    @VariableSource("ALL_TYPES_COLUMN_DEFINITIONS")
    void testCreateIndex(ColumnDefinition columnDefinition) throws Exception {
        testPutGetRemove(List.of(columnDefinition));
    }

    /**
     * Tests the Put-Get-Remove case when an index is created using all possible column in random order.
     */
    @RepeatedTest(5)
    void testCreateMultiColumnIndex() throws Exception {
        testPutGetRemove(shuffledDefinitions());
    }

    /**
     * Tests the happy case of the {@link SortedIndexStorage#range} method.
     */
    @RepeatedTest(5)
    void testRange() throws Exception {
        List<ColumnDefinition> indexSchema = shuffledDefinitions();

        SortedIndexStorage indexStorage = createIndex(indexSchema);

        List<IndexRowWrapper> entries = IntStream.range(0, 10)
                .mapToObj(i -> {
                    IndexRowWrapper entry = IndexRowWrapper.randomRow(indexStorage);

                    indexStorage.put(entry.row());

                    return entry;
                })
                .sorted()
                .collect(toList());

        int firstIndex = 3;
        int lastIndex = 8;

        List<byte[]> expected = entries.stream()
                .skip(firstIndex)
                .limit(lastIndex - firstIndex + 1)
                .map(e -> e.row().primaryKey().keyBytes())
                .collect(toList());

        IndexRowPrefix first = entries.get(firstIndex).prefix(3);
        IndexRowPrefix last = entries.get(lastIndex).prefix(5);

        List<byte[]> actual = cursorToList(indexStorage.range(first, last))
                .stream()
                .map(IndexRow::primaryKey)
                .map(SearchRow::keyBytes)
                .collect(toList());

        assertThat(actual, hasSize(lastIndex - firstIndex + 1));

        for (int i = firstIndex; i < actual.size(); ++i) {
            assertThat(actual.get(i), is(equalTo(expected.get(i))));
        }
    }

    /**
     * Tests that an empty range is returned if {@link SortedIndexStorage#range} method is called using overlapping keys.
     */
    @Test
    void testEmptyRange() throws Exception {
        List<ColumnDefinition> indexSchema = shuffledRandomDefinitions();

        SortedIndexStorage indexStorage = createIndex(indexSchema);

        IndexRowWrapper entry1 = IndexRowWrapper.randomRow(indexStorage);
        IndexRowWrapper entry2 = IndexRowWrapper.randomRow(indexStorage);

        if (entry2.compareTo(entry1) < 0) {
            IndexRowWrapper t = entry2;
            entry2 = entry1;
            entry1 = t;
        }

        indexStorage.put(entry1.row());
        indexStorage.put(entry2.row());

        List<IndexRow> actual = cursorToList(indexStorage.range(entry2::columns, entry1::columns));

        assertThat(actual, is(empty()));
    }

    /**
     * Tests creating a index that has not been created through the Configuration framework.
     */
    @Test
    void testCreateMissingIndex() {
        StorageException ex = assertThrows(StorageException.class, () -> tableStorage.getOrCreateSortedIndex("does not exist"));

        assertThat(ex.getMessage(), is(equalTo("Index configuration for \"does not exist\" could not be found")));
    }

    /**
     * Tests creating a Sorted Index that has been misconfigured as a Hash Index.
     */
    @Test
    void testCreateMisconfiguredIndex() {
        HashIndexDefinition definition = SchemaBuilders.hashIndex("wrong type")
                .withColumns("foo")
                .build();

        StorageException ex = assertThrows(StorageException.class, () -> createIndex(definition));

        assertThat(ex.getMessage(), is(equalTo("Index \"WRONG TYPE\" is not configured as a Sorted Index. Actual type: HASH")));
    }

    /**
     * Tests the {@link TableStorage#dropIndex} functionality.
     */
    @Test
    void testDropIndex() throws Exception {
        SortedIndexStorage storage = createIndex(ALL_TYPES_COLUMN_DEFINITIONS.subList(0, 1));

        String indexName = storage.indexDescriptor().name();

        assertThat(tableStorage.getOrCreateSortedIndex(indexName), is(sameInstance(storage)));

        IndexRowWrapper entry = IndexRowWrapper.randomRow(storage);

        storage.put(entry.row());

        tableStorage.dropIndex(indexName);

        SortedIndexStorage nextStorage = tableStorage.getOrCreateSortedIndex(indexName);

        assertThat(nextStorage, is(not(sameInstance(storage))));
        assertThat(getSingle(nextStorage, entry), is(nullValue()));
    }

    @ParameterizedTest
    @VariableSource("ALL_TYPES_COLUMN_DEFINITIONS")
    void testNullValues(ColumnDefinition columnDefinition) throws Exception {
        SortedIndexStorage storage = createIndex(List.of(columnDefinition));

        IndexRowWrapper entry1 = IndexRowWrapper.randomRow(storage);

        Object[] nullArray = storage.indexDescriptor().indexRowColumns().stream()
                .map(columnDescriptor -> columnDescriptor.indexedColumn() ? null : (byte) random.nextInt())
                .toArray();

        IndexRow nullRow = storage.indexRowFactory().createIndexRow(nullArray, new ByteArraySearchRow(randomBytes(random, 10)));

        IndexRowWrapper entry2 = new IndexRowWrapper(storage, nullRow, nullArray);

        storage.put(entry1.row());
        storage.put(entry2.row());

        if (entry1.compareTo(entry2) > 0) {
            IndexRowWrapper t = entry2;
            entry2 = entry1;
            entry1 = t;
        }

        List<IndexRow> rows = cursorToList(storage.range(entry1::columns, entry2::columns));

        assertThat(rows, contains(entry1.row(), entry2.row()));
    }

    private List<ColumnDefinition> shuffledRandomDefinitions() {
        return shuffledDefinitions(d -> random.nextBoolean());
    }

    private List<ColumnDefinition> shuffledDefinitions() {
        return shuffledDefinitions(d -> true);
    }

    private List<ColumnDefinition> shuffledDefinitions(Predicate<ColumnDefinition> filter) {
        List<ColumnDefinition> shuffledDefinitions = ALL_TYPES_COLUMN_DEFINITIONS.stream()
                .filter(filter)
                .collect(toList());

        Collections.shuffle(shuffledDefinitions, random);

        if (log.isInfoEnabled()) {
            List<String> columnNames = shuffledDefinitions.stream().map(ColumnDefinition::name).collect(toList());

            log.info("Creating index with the following column order: " + columnNames);
        }

        return shuffledDefinitions;
    }

    /**
     * Tests the Get-Put-Remove scenario: inserts some keys into the storage and checks that they have been successfully persisted and can
     * be removed.
     */
    private void testPutGetRemove(List<ColumnDefinition> indexSchema) throws Exception {
        SortedIndexStorage indexStorage = createIndex(indexSchema);

        IndexRowWrapper entry1 = IndexRowWrapper.randomRow(indexStorage);
        IndexRowWrapper entry2;

        // using a cycle here to protect against equal keys being generated
        do {
            entry2 = IndexRowWrapper.randomRow(indexStorage);
        } while (entry1.equals(entry2));

        indexStorage.put(entry1.row());
        indexStorage.put(entry2.row());

        assertThat(
                getSingle(indexStorage, entry1).primaryKey().keyBytes(),
                is(equalTo(entry1.row().primaryKey().keyBytes()))
        );

        assertThat(
                getSingle(indexStorage, entry2).primaryKey().keyBytes(),
                is(equalTo(entry2.row().primaryKey().keyBytes()))
        );

        indexStorage.remove(entry1.row());

        assertThat(getSingle(indexStorage, entry1), is(nullValue()));
    }

    /**
     * Creates a Sorted Index using the given columns.
     */
    private SortedIndexStorage createIndex(List<ColumnDefinition> indexSchema) {
        SortedIndexDefinitionBuilder indexDefinitionBuilder = SchemaBuilders.sortedIndex("foo");

        indexSchema.forEach(column -> {
            SortedIndexColumnBuilder columnBuilder = indexDefinitionBuilder.addIndexColumn(column.name());

            if (random.nextBoolean()) {
                columnBuilder.asc();
            } else {
                columnBuilder.desc();
            }

            columnBuilder.done();
        });

        SortedIndexDefinition indexDefinition = indexDefinitionBuilder.build();

        return createIndex(indexDefinition);
    }

    /**
     * Creates a Sorted Index using the given index definition.
     */
    private SortedIndexStorage createIndex(ColumnarIndexDefinition indexDefinition) {
        CompletableFuture<Void> createIndexFuture = tableCfg.change(cfg ->
                cfg.changeIndices(idxList ->
                        idxList.create(indexDefinition.name(), idx -> convert(indexDefinition, idx))));

        assertThat(createIndexFuture, willBe(nullValue(Void.class)));

        return tableStorage.getOrCreateSortedIndex(indexDefinition.name());
    }

    /**
     * Extracts all data from a given cursor and closes it.
     */
    private static <T> List<T> cursorToList(Cursor<T> cursor) throws Exception {
        try (cursor) {
            var list = new ArrayList<T>();

            cursor.forEachRemaining(list::add);

            return list;
        }
    }

    /**
     * Extracts a single value by a given key or {@code null} if it does not exist.
     */
    @Nullable
    private static IndexRow getSingle(SortedIndexStorage indexStorage, IndexRowWrapper entry) throws Exception {
        IndexRowPrefix fullPrefix = entry::columns;

        List<IndexRow> values = cursorToList(indexStorage.range(fullPrefix, fullPrefix));

        assertThat(values, anyOf(empty(), hasSize(1)));

        return values.isEmpty() ? null : values.get(0);
    }
}
