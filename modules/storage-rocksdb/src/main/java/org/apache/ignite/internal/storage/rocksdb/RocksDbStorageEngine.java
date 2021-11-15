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

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.schemas.store.DataRegionConfiguration;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.DataRegion;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.rocksdb.RocksDB;

/**
 * Storage engine implementation based on RocksDB.
 */
public class RocksDbStorageEngine implements StorageEngine {
    static {
        RocksDB.loadLibrary();
    }

    /**
     * Thread pool to be used to snapshot partitions and maybe some other operations.
     */
    private final ExecutorService threadPool = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(), new NamedThreadFactory("rocksdb-storage-engine-pool"));

    /** {@inheritDoc} */
    @Override
    public void start() {
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws StorageException {
        IgniteUtils.shutdownAndAwaitTermination(threadPool, 10, TimeUnit.SECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public DataRegion createDataRegion(DataRegionConfiguration regionCfg) {
        return new RocksDbDataRegion(regionCfg);
    }

    /** {@inheritDoc} */
    @Override
    public TableStorage createTable(
            Path tablePath,
            TableConfiguration tableCfg,
            DataRegion dataRegion,
            BiFunction<TableView, String, Comparator<ByteBuffer>> indexComparatorFactory
    ) {
        assert dataRegion instanceof RocksDbDataRegion : dataRegion;

        return new RocksDbTableStorage(
                tablePath,
                tableCfg,
                threadPool,
                (RocksDbDataRegion) dataRegion,
                indexComparatorFactory
        );
    }
}
