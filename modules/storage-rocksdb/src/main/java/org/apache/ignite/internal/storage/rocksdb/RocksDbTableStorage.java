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

import static java.lang.Integer.parseInt;
import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.DataRegion;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.AbstractComparator;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Table storage implementation based on {@link RocksDB} instance.
 */
public class RocksDbTableStorage implements TableStorage {
    /**
     * Name of the meta column family matches default columns family, meaning that it always exist when new table is created.
     */
    private static final String CF_META = "default";

    /** Prefix for partitions column families names. */
    private static final String CF_PARTITION_PREFIX = "cf-part-";

    /** Prefix for SQL indexes column family names. */
    private static final String CF_INDEX_PREFIX = "cf-idx-";

    /** Name of comparator used in indexes column family. */
    private static final String INDEX_COMPARATOR_NAME = "index-comparator";

    /** Path for the directory that stores table data. */
    private final Path tablePath;

    /** Table configuration. */
    private final TableConfiguration tableCfg;

    /** Data region for the table. */
    private final RocksDbDataRegion dataRegion;

    /** Comparators factory for indexes. */
    private final BiFunction<TableView, String, Comparator<ByteBuffer>> indexComparatorFactory;

    /** List of closeable resources to close on {@link #stop()}. Better than having a field for each one of them. */
    private final List<AutoCloseable> autoCloseables = new ArrayList<>();

    /** Rocks DB instance itself. */
    private RocksDB db;

    /** CF handle for meta information. */
    @SuppressWarnings("unused")
    private ColumnFamilyHandle metaCfHandle;

    /** Column families for partitions. Stored as an array for the quick access by an index. */
    private AtomicReferenceArray<RocksDbPartitionStorage> partitions;

    /** Column families for indexes by their names. */
    private final Map<String, ColumnFamilyHandle> indicesCfHandles = new ConcurrentHashMap<>();

    /**
     *
     */
    private boolean stopped = false;

    /** Utility enum to describe a type of the column family - meta, partition or index. */
    private enum ColumnFamilyType {
        META, PARTITION, INDEX
    }

    /**
     * Constructor.
     *
     * @param tablePath              Path for the directory that stores table data.
     * @param tableCfg               Table configuration.
     * @param dataRegion             Data region for the table.
     * @param indexComparatorFactory Comparators factory for indexes.
     */
    public RocksDbTableStorage(
            Path tablePath,
            TableConfiguration tableCfg,
            RocksDbDataRegion dataRegion,
            BiFunction<TableView, String, Comparator<ByteBuffer>> indexComparatorFactory
    ) {
        this.tablePath = tablePath;
        this.tableCfg = tableCfg;
        this.dataRegion = dataRegion;
        this.indexComparatorFactory = indexComparatorFactory;
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
            throw new StorageException("Failed to create directory for table storage.", e);
        }

        Map<ColumnFamilyType, List<String>> cfNamesGrouped = getColumnFamiliesNames();

        List<ColumnFamilyDescriptor> cfDescriptors = convertToColumnFamiliesDescriptors(cfNamesGrouped);

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

        DBOptions dbOptions = addToCloseableResources(new DBOptions()
                .setCreateIfMissing(true)
                .setWriteBufferManager(dataRegion.writeBufferManager())
        );

        partitions = new AtomicReferenceArray<>(tableCfg.value().partitions());

        try {
            db = addToCloseableResources(RocksDB.open(dbOptions, tablePath.toAbsolutePath().toString(), cfDescriptors, cfHandles));
        } catch (RocksDBException e) {
            throw new StorageException("Failed to initialize RocksDB instance.", e);
        }

        for (int cfListIndex = 0; cfListIndex < cfHandles.size(); cfListIndex++) {
            ColumnFamilyHandle cfHandle = cfHandles.get(cfListIndex);

            String handleName;
            try {
                handleName = new String(cfHandle.getName(), StandardCharsets.UTF_8);
            } catch (RocksDBException e) {
                throw new StorageException("Failed to read RocksDB column family name.", e);
            }

            if (handleName.equals(CF_META)) {
                this.metaCfHandle = addToCloseableResources(cfHandle);
            } else if (handleName.startsWith(CF_PARTITION_PREFIX)) {
                int partId = partitionId(handleName);

                ColumnFamilyDescriptor cfDescriptor = cfDescriptors.get(cfListIndex);

                ColumnFamily cf = new ColumnFamily(db, cfHandle, handleName, cfDescriptor.getOptions(), null);

                partitions.set(partId, new RocksDbPartitionStorage(partId, db, cf));
            } else {
                String indexName = handleName.substring(CF_INDEX_PREFIX.length());

                indicesCfHandles.put(indexName, cfHandle);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws StorageException {
        try {
            stopped = true;

            List<AutoCloseable> resources = new ArrayList<>();

            resources.addAll(autoCloseables);
            resources.addAll(indicesCfHandles.values());

            for (int i = 0; i < partitions.length(); i++) {
                RocksDbPartitionStorage partition = partitions.get(i);

                if (partition != null) {
                    resources.add(partition);
                }
            }

            Collections.reverse(resources);

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
        if (stopped) {
            throw new StorageException(new NodeStoppingException());
        }

        checkPartitionId(partId);

        RocksDbPartitionStorage partition = partitions.get(partId);

        if (partition == null) {
            String handleName = partitionColumnFamilyName(partId);

            ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(
                    handleName.getBytes(StandardCharsets.UTF_8),
                    new ColumnFamilyOptions()
            );

            try {
                ColumnFamilyHandle cfHandle = db.createColumnFamily(cfDescriptor);

                ColumnFamily cf = new ColumnFamily(db, cfHandle, handleName, cfDescriptor.getOptions(), null);

                partition = new RocksDbPartitionStorage(partId, db, cf);
            } catch (RocksDBException e) {
                cfDescriptor.getOptions().close();

                throw new StorageException("Failed to create new RocksDB column family " + handleName, e);
            }

            partitions.set(partId, partition);
        }

        return partition;
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
        if (stopped) {
            throw new StorageException(new NodeStoppingException());
        }

        checkPartitionId(partId);

        RocksDbPartitionStorage partition = partitions.get(partId);

        if (partition != null) {
            partitions.set(partId, null);

            ColumnFamily cf = partition.columnFamily();

            ColumnFamilyHandle cfHandle = cf.handle();

            try {
                db.dropColumnFamily(cfHandle);

                db.destroyColumnFamilyHandle(cfHandle);
            } catch (RocksDBException e) {
                throw new StorageException("Failed to stop partition", e);
            }
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
     * Returns list of column families names that belong to RocksDB instance in the given path, grouped by thier {@link ColumnFamilyType}.
     *
     * @return Map with column families names.
     * @throws StorageException If something went wrong.
     */
    private Map<ColumnFamilyType, List<String>> getColumnFamiliesNames() {
        String absolutePathStr = tablePath.toAbsolutePath().toString();

        List<String> cfNames = new ArrayList<>();

        try (Options opts = new Options()) {
            List<byte[]> cfNamesBytes = RocksDB.listColumnFamilies(opts, absolutePathStr);

            for (byte[] cfNameBytes : cfNamesBytes) {
                cfNames.add(new String(cfNameBytes, StandardCharsets.UTF_8));
            }
        } catch (RocksDBException e) {
            throw new StorageException(
                    "Failed to read list of column families names for the RocksDB instance located at path " + absolutePathStr,
                    e
            );
        }

        return cfNames.stream().collect(groupingBy(this::columnFamilyType));
    }

    /**
     * Returns list of CF descriptors by their names.
     *
     * @param cfGrouped Map from CF type to lists of names.
     * @return List of CF descriptors.
     */
    @NotNull
    private List<ColumnFamilyDescriptor> convertToColumnFamiliesDescriptors(
            Map<ColumnFamilyType,
                    List<String>> cfGrouped
    ) {
        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

        Options cfOptions = addToCloseableResources(new Options().setCreateIfMissing(true));

        cfDescriptors.add(new ColumnFamilyDescriptor(
                CF_META.getBytes(StandardCharsets.UTF_8),
                addToCloseableResources(new ColumnFamilyOptions(cfOptions))
        ));

        for (String partitionCfName : cfGrouped.getOrDefault(ColumnFamilyType.PARTITION, List.of())) {
            cfDescriptors.add(new ColumnFamilyDescriptor(
                    partitionCfName.getBytes(StandardCharsets.UTF_8),
                    new ColumnFamilyOptions()
            ));
        }

        NamedListView<? extends TableIndexView> indicesCfgView = tableCfg.value().indices();

        for (String indexCfName : cfGrouped.getOrDefault(ColumnFamilyType.INDEX, List.of())) {
            String indexName = indexCfName.substring(CF_INDEX_PREFIX.length());

            TableIndexView indexCfgView = indicesCfgView.get(indexName);

            assert indexCfgView != null : "Found index that is absent in configuration: " + indexCfName;

            Comparator<ByteBuffer> indexComparator = indexComparatorFactory.apply(tableCfg.value(), indexName);

            cfDescriptors.add(new ColumnFamilyDescriptor(
                    indexCfName.getBytes(StandardCharsets.UTF_8),
                    new ColumnFamilyOptions()
                            .setComparator(addToCloseableResources(
                                    new AbstractComparator(addToCloseableResources(new ComparatorOptions())) {
                                        /** {@inheritDoc} */
                                        @Override
                                        public String name() {
                                            return INDEX_COMPARATOR_NAME;
                                        }

                                        /** {@inheritDoc} */
                                        @Override
                                        public int compare(ByteBuffer a, ByteBuffer b) {
                                            return indexComparator.compare(a, b);
                                        }
                                    }))
            ));
        }

        return cfDescriptors;
    }

    /**
     * Creates column family name by partition id.
     *
     * @param partId Partition id.
     * @return Column family name.
     */
    private static String partitionColumnFamilyName(int partId) {
        return CF_PARTITION_PREFIX + partId;
    }

    /**
     * Gets partition id from column family name.
     *
     * @param cfName Column family name.
     * @return Partition id.
     */
    private static int partitionId(String cfName) {
        return parseInt(cfName.substring(CF_PARTITION_PREFIX.length()));
    }

    /**
     * Creates column family name by index name.
     *
     * @param idxName Index name.
     * @return Column family name.
     */
    private static String indexColumnFamilyName(String idxName) {
        return CF_INDEX_PREFIX + idxName;
    }

    /**
     * Determines column family type by its name.
     *
     * @param cfName Column family name.
     * @return Column family type.
     * @throws StorageException If column family name doesn't match any known pattern.
     */
    private ColumnFamilyType columnFamilyType(String cfName) throws StorageException {
        if (CF_META.equals(cfName)) {
            return ColumnFamilyType.META;
        }

        if (cfName.startsWith(CF_PARTITION_PREFIX)) {
            return ColumnFamilyType.PARTITION;
        }

        if (cfName.startsWith(CF_INDEX_PREFIX)) {
            return ColumnFamilyType.INDEX;
        }

        throw new StorageException("Unidentified column family [name=" + cfName + ", table=" + tableCfg.name() + ']');
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
