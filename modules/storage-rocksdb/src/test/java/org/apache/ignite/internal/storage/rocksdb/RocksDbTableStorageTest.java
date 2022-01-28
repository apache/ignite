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

package org.apache.ignite.internal.storage.rocksdb;

import static org.apache.ignite.internal.configuration.ConfigurationTestUtils.fixConfiguration;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.schemas.store.DataRegionConfiguration;
import org.apache.ignite.configuration.schemas.store.RocksDbDataRegionChange;
import org.apache.ignite.configuration.schemas.store.RocksDbDataRegionConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.storage.engine.DataRegion;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for the {@link RocksDbTableStorage}.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class RocksDbTableStorageTest {
    @WorkDirectory
    private Path workDir;

    private final StorageEngine engine = new RocksDbStorageEngine();

    private TableStorage storage;

    private DataRegion dataRegion;

    @BeforeEach
    public void setUp(
            @InjectConfiguration(polymorphicExtensions = RocksDbDataRegionConfigurationSchema.class) DataRegionConfiguration dataRegionCfg,
            @InjectConfiguration(polymorphicExtensions = HashIndexConfigurationSchema.class) TableConfiguration tableCfg
    ) throws Exception {
        CompletableFuture<Void> changeFuture = dataRegionCfg.change(cfg ->
                cfg.convert(RocksDbDataRegionChange.class).changeSize(16 * 1024).changeWriteBufferSize(16 * 1024)
        );

        assertThat(changeFuture, willBe(nullValue(Void.class)));

        changeFuture = tableCfg.change(cfg -> cfg.changePartitions(512));

        assertThat(changeFuture, willBe(nullValue(Void.class)));

        dataRegionCfg = fixConfiguration(dataRegionCfg);

        dataRegion = engine.createDataRegion(dataRegionCfg);

        assertThat(dataRegion, is(instanceOf(RocksDbDataRegion.class)));

        dataRegion.start();

        storage = engine.createTable(workDir, tableCfg, dataRegion);

        assertThat(storage, is(instanceOf(RocksDbTableStorage.class)));

        storage.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        IgniteUtils.closeAll(
                storage == null ? null : storage::stop,
                dataRegion == null ? null : dataRegion::stop,
                engine::stop
        );
    }

    /**
     * Tests that {@link RocksDbTableStorage#getPartition} correctly returns an existing partition.
     */
    @Test
    void testCreatePartition() {
        PartitionStorage partitionStorage = storage.getPartition(0);

        assertThat(partitionStorage, is(nullValue()));

        partitionStorage = storage.getOrCreatePartition(0);

        assertThat(partitionStorage, is(notNullValue()));

        var testData = new SimpleDataRow("foo".getBytes(StandardCharsets.UTF_8), "bar".getBytes(StandardCharsets.UTF_8));

        partitionStorage.write(testData);

        partitionStorage = storage.getPartition(0);

        assertThat(partitionStorage, is(notNullValue()));

        assertThat(partitionStorage.read(testData), is(equalTo(testData)));
    }

    /**
     * Tests that partition data does not overlap.
     */
    @Test
    void testPartitionIndependence() throws Exception {
        PartitionStorage partitionStorage0 = storage.getOrCreatePartition(42);
        // using a shifted ID value to test a multi-byte scenario
        PartitionStorage partitionStorage1 = storage.getOrCreatePartition(1 << 8);

        var testData = new SimpleDataRow("foo".getBytes(StandardCharsets.UTF_8), "bar".getBytes(StandardCharsets.UTF_8));

        partitionStorage0.write(testData);

        assertThat(partitionStorage0.read(testData), is(equalTo(testData)));
        assertThat(partitionStorage1.read(testData), is(nullValue()));

        var testData2 = new SimpleDataRow("baz".getBytes(StandardCharsets.UTF_8), "quux".getBytes(StandardCharsets.UTF_8));

        partitionStorage1.write(testData2);

        assertThat(partitionStorage0.read(testData2), is(nullValue()));
        assertThat(partitionStorage1.read(testData2), is(equalTo(testData2)));

        assertThat(toList(partitionStorage0.scan(row -> true)), contains(testData));
        assertThat(toList(partitionStorage1.scan(row -> true)), contains(testData2));
    }

    private static <T> List<T> toList(Cursor<T> cursor) throws Exception {
        var list = new ArrayList<T>();

        try (cursor) {
            cursor.forEach(list::add);
        }

        return list;
    }

    /**
     * Tests that dropping a partition does not remove extra data.
     */
    @Test
    void testDropPartition() {
        var testData = new SimpleDataRow("foo".getBytes(StandardCharsets.UTF_8), "bar".getBytes(StandardCharsets.UTF_8));

        storage.getOrCreatePartition(42).write(testData);
        storage.getOrCreatePartition(1 << 8).write(testData);

        storage.dropPartition(42);

        assertThat(storage.getPartition(42), is(nullValue()));
        assertThat(storage.getOrCreatePartition(42).read(testData), is(nullValue()));
        assertThat(storage.getPartition(1 << 8).read(testData), is(equalTo(testData)));
    }

    /**
     * Tests that restarting the storage does not result in data loss.
     */
    @Test
    void testRestart(
            @InjectConfiguration(polymorphicExtensions = HashIndexConfigurationSchema.class) TableConfiguration tableCfg
    ) {
        var testData = new SimpleDataRow("foo".getBytes(StandardCharsets.UTF_8), "bar".getBytes(StandardCharsets.UTF_8));

        storage.getOrCreatePartition(0).write(testData);

        storage.stop();

        storage = engine.createTable(workDir, tableCfg, dataRegion);

        storage.start();

        assertThat(storage.getPartition(0), is(notNullValue()));
        assertThat(storage.getPartition(1), is(nullValue()));
        assertThat(storage.getPartition(0).read(testData), is(equalTo(testData)));
    }
}
