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

package org.apache.ignite.internal.storage.rocksdb;

import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.columnFamilyType;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.partitionCfName;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.partitionId;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.sortedIndexCfName;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.sortedIndexName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.DataRegion;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.index.BinaryRowComparator;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbSortedIndexStorage;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Table storage implementation based on {@link RocksDB} instance.
 */
public class RocksDbTableStorage implements TableStorage {
    /** Path for the directory that stores table data. */
    private final Path tablePath;

    /** Table configuration. */
    private final TableConfiguration tableCfg;

    /** Thread pool for async operations. */
    private final Executor threadPool;

    /** Data region for the table. */
    private final RocksDbDataRegion dataRegion;

    /** List of closeable resources to close on {@link #stop()}. Better than having a field for each one of them. */
    private final List<AutoCloseable> autoCloseables = new ArrayList<>();

    /** Rocks DB instance. */
    private RocksDB db;

    /** CF handle for meta information. */
    @SuppressWarnings("unused")
    private ColumnFamily metaCf;

    /** Column families for partitions. Stored as an array for the quick access by an index. */
    private AtomicReferenceArray<RocksDbPartitionStorage> partitions;

    /** Column families for indexes by their names. */
    private final Map<String, RocksDbSortedIndexStorage> sortedIndices = new ConcurrentHashMap<>();

    private boolean stopped = false;

    /**
     * Constructor.
     *
     * @param tablePath  Path for the directory that stores table data.
     * @param tableCfg   Table configuration.
     * @param threadPool Thread pool for async operations.
     * @param dataRegion Data region for the table.
     */
    public RocksDbTableStorage(
            Path tablePath,
            TableConfiguration tableCfg,
            Executor threadPool,
            RocksDbDataRegion dataRegion
    ) {
        this.tablePath = tablePath;
        this.tableCfg = tableCfg;
        this.threadPool = threadPool;
        this.dataRegion = dataRegion;
    }

    /** {@inheritDoc} */
    @Override
    public TableConfiguration configuration() {
        return tableCfg;
    }

    /** {@inheritDoc} */
    @Override
    public DataRegion dataRegion() {
        return dataRegion;
    }

    /** {@inheritDoc} */
    @Override
    public void start() throws StorageException {
        try {
            Files.createDirectories(tablePath);
        } catch (IOException e) {
            throw new StorageException("Failed to create a directory for the table storage.", e);
        }

        List<ColumnFamilyDescriptor> cfDescriptors = getExistingCfDescriptors();

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());

        DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setWriteBufferManager(dataRegion.writeBufferManager());

        partitions = new AtomicReferenceArray<>(tableCfg.value().partitions());

        try {
            db = RocksDB.open(dbOptions, tablePath.toAbsolutePath().toString(), cfDescriptors, cfHandles);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to initialize RocksDB instance.", e);
        }

        addToCloseableResources(db::closeE);

        // read all existing Column Families from the db and parse them according to type: meta, partition data or index.
        for (int i = 0; i < cfHandles.size(); i++) {
            ColumnFamilyHandle cfHandle = cfHandles.get(i);

            ColumnFamilyDescriptor cfDescriptor = cfDescriptors.get(i);

            String handleName = cfHandleName(cfHandle);

            ColumnFamily cf = new ColumnFamily(db, cfHandle, handleName, cfDescriptor.getOptions(), null);

            switch (columnFamilyType(handleName)) {
                case META:
                    metaCf = addToCloseableResources(cf);

                    break;

                case PARTITION:
                    int partId = partitionId(handleName);

                    partitions.set(partId, new RocksDbPartitionStorage(threadPool, partId, db, cf));

                    break;

                case SORTED_INDEX:
                    String indexName = sortedIndexName(handleName);

                    var indexDescriptor = new SortedIndexDescriptor(indexName, tableCfg.value());

                    sortedIndices.put(indexName, new RocksDbSortedIndexStorage(cf, indexDescriptor));

                    break;

                default:
                    throw new StorageException("Unidentified column family [name=" + handleName + ", table=" + tableCfg.name() + ']');
            }
        }
    }

    /**
     * Extracts the Column Family name from the given handle.
     */
    private static String cfHandleName(ColumnFamilyHandle handle) {
        try {
            return new String(handle.getName(), StandardCharsets.UTF_8);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to read RocksDB column family name.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws StorageException {
        stopped = true;

        List<AutoCloseable> resources = new ArrayList<>();

        resources.addAll(autoCloseables);

        resources.addAll(sortedIndices.values());

        for (int i = 0; i < partitions.length(); i++) {
            RocksDbPartitionStorage partition = partitions.get(i);

            if (partition != null) {
                resources.add(partition);
            }
        }

        Collections.reverse(resources);

        try {
            IgniteUtils.closeAll(resources);
        } catch (Exception e) {
            throw new StorageException("Failed to stop RocksDB table storage.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() throws StorageException {
        stop();

        IgniteUtils.deleteIfExists(tablePath);
    }

    /** {@inheritDoc} */
    @Override
    public PartitionStorage getOrCreatePartition(int partId) throws StorageException {
        PartitionStorage partition = getPartition(partId);

        if (partition != null) {
            return partition;
        }

        String handleName = partitionCfName(partId);

        ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(
                handleName.getBytes(StandardCharsets.UTF_8),
                new ColumnFamilyOptions()
        );

        try {
            ColumnFamilyHandle cfHandle = db.createColumnFamily(cfDescriptor);

            ColumnFamily cf = new ColumnFamily(db, cfHandle, handleName, cfDescriptor.getOptions(), null);

            var newPartition = new RocksDbPartitionStorage(threadPool, partId, db, cf);

            partitions.set(partId, newPartition);

            return newPartition;
        } catch (RocksDBException e) {
            cfDescriptor.getOptions().close();

            throw new StorageException("Failed to create new RocksDB column family " + handleName, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public PartitionStorage getPartition(int partId) {
        if (stopped) {
            throw new StorageException(new NodeStoppingException());
        }

        checkPartitionId(partId);

        return partitions.get(partId);
    }

    /** {@inheritDoc} */
    @Override
    public void dropPartition(int partId) throws StorageException {
        PartitionStorage partition = getPartition(partId);

        if (partition != null) {
            partitions.set(partId, null);

            partition.destroy();
        }
    }

    @Override
    public SortedIndexStorage getOrCreateSortedIndex(String indexName) {
        if (stopped) {
            throw new StorageException(new NodeStoppingException());
        }

        return sortedIndices.computeIfAbsent(indexName, name -> {
            var indexDescriptor = new SortedIndexDescriptor(name, tableCfg.value());

            ColumnFamilyDescriptor cfDescriptor = sortedIndexCfDescriptor(indexDescriptor);

            ColumnFamily cf = createColumnFamily(sortedIndexCfName(name), cfDescriptor);

            return new RocksDbSortedIndexStorage(cf, indexDescriptor);
        });
    }

    @Override
    public void dropIndex(String indexName) {
        if (stopped) {
            throw new StorageException(new NodeStoppingException());
        }

        sortedIndices.computeIfPresent(indexName, (name, indexStorage) -> {
            indexStorage.destroy();

            return null;
        });
    }

    /**
     * Creates a Column Family using the given descriptor.
     */
    private ColumnFamily createColumnFamily(String cfName, ColumnFamilyDescriptor cfDescriptor) {
        try {
            ColumnFamilyHandle cfHandle = db.createColumnFamily(cfDescriptor);

            return new ColumnFamily(db, cfHandle, cfName, cfDescriptor.getOptions(), null);
        } catch (RocksDBException e) {
            cfDescriptor.getOptions().close();

            throw new StorageException("Failed to create new RocksDB column family: " + cfName, e);
        }
    }

    /**
     * Checks that a passed partition id is within the proper bounds.
     *
     * @param partId Partition id.
     */
    private void checkPartitionId(int partId) {
        if (partId < 0 || partId >= partitions.length()) {
            throw new IllegalArgumentException(S.toString(
                    "Unable to access partition with id outside of configured range",
                    "table", tableCfg.name().value(), false,
                    "partitionId", partId, false,
                    "partitions", partitions.length(), false
            ));
        }
    }

    /**
     * Returns a list of Column Families' names that belong to a RocksDB instance in the given path.
     *
     * @return Map with column families names.
     * @throws StorageException If something went wrong.
     */
    private List<String> getExistingCfNames() {
        String absolutePathStr = tablePath.toAbsolutePath().toString();

        try (Options opts = new Options()) {
            List<String> existingNames = RocksDB.listColumnFamilies(opts, absolutePathStr)
                    .stream()
                    .map(cfNameBytes -> new String(cfNameBytes, StandardCharsets.UTF_8))
                    .collect(Collectors.toList());

            // even if the database is new (no existing Column Families), we should still return the default Column Family,
            // which happens to be the same as the Meta Column Family.
            return existingNames.isEmpty() ? List.of(ColumnFamilyUtils.CF_META) : existingNames;
        } catch (RocksDBException e) {
            throw new StorageException(
                    "Failed to read list of column families names for the RocksDB instance located at path " + absolutePathStr, e
            );
        }
    }

    /**
     * Returns a list of CF descriptors present in the RocksDB instance.
     *
     * @return List of CF descriptors.
     */
    @NotNull
    private List<ColumnFamilyDescriptor> getExistingCfDescriptors() {
        return getExistingCfNames().stream()
                .map(this::cfDescriptorFromName)
                .collect(Collectors.toList());
    }

    /**
     * Creates a Column Family descriptor for the given Family type (encoded in its name).
     */
    private ColumnFamilyDescriptor cfDescriptorFromName(String cfName) {
        switch (columnFamilyType(cfName)) {
            case META:
            case PARTITION:
                return new ColumnFamilyDescriptor(cfName.getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions());

            case SORTED_INDEX:
                var indexDescriptor = new SortedIndexDescriptor(sortedIndexName(cfName), tableCfg.value());

                return sortedIndexCfDescriptor(indexDescriptor);

            default:
                throw new StorageException("Unidentified column family [name=" + cfName + ", table=" + tableCfg.name() + ']');
        }
    }

    /**
     * Creates a Column Family descriptor for a Sorted Index.
     */
    private static ColumnFamilyDescriptor sortedIndexCfDescriptor(SortedIndexDescriptor descriptor) {
        String cfName = sortedIndexCfName(descriptor.name());

        ColumnFamilyOptions options = new ColumnFamilyOptions().setComparator(new BinaryRowComparator(descriptor));

        return new ColumnFamilyDescriptor(cfName.getBytes(StandardCharsets.UTF_8), options);
    }

    /**
     * Adds resource to the {@link #autoCloseables} list.
     *
     * @param autoCloseable Closeable resource.
     * @param <R>           Type of the resource.
     * @return Passed resource with the same type.
     */
    private <R extends AutoCloseable> R addToCloseableResources(R autoCloseable) {
        autoCloseables.add(autoCloseable);

        return autoCloseable;
    }
}
