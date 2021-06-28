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

package org.apache.ignite.internal.vault.persistence;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultService;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompactionPriority;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Vault Service implementation based on <a href="https://github.com/facebook/rocksdb">RocksDB</a>.
 */
public class PersistentVaultService implements VaultService {
    static {
        RocksDB.loadLibrary();
    }

    /** */
    private final ExecutorService threadPool = Executors.newFixedThreadPool(2);

    /** */
    private final Options options = new Options();

    /** */
    private final RocksDB db;

    /**
     * Creates and starts the RocksDB instance using the recommended options on the given {@code path}.
     *
     * @param path base path for RocksDB
     */
    public PersistentVaultService(Path path) {
        // using the recommended options from https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning
        options
            .setCreateIfMissing(true)
            .setCompressionType(CompressionType.LZ4_COMPRESSION)
            .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
            .setLevelCompactionDynamicLevelBytes(true)
            .setBytesPerSync(1048576)
            .setCompactionPriority(CompactionPriority.MinOverlappingRatio)
            .setTableFormatConfig(
                new BlockBasedTableConfig()
                    .setBlockSize(16 * 1024)
                    .setCacheIndexAndFilterBlocks(true)
                    .setPinL0FilterAndIndexBlocksInCache(true)
                    .setFormatVersion(5)
                    .setFilterPolicy(new BloomFilter(10, false))
                    .setOptimizeFiltersForMemory(true)
            );

        try {
            db = RocksDB.open(options, path.toString());
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws RocksDBException {
        try (options; db) {
            db.syncWal();

            IgniteUtils.shutdownAndAwaitTermination(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<VaultEntry> get(@NotNull ByteArray key) {
        return supplyAsync(() -> db.get(key.bytes()))
            .thenApply(v -> new VaultEntry(key, v));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> put(@NotNull ByteArray key, byte @Nullable [] val) {
        return val == null ? remove(key) : runAsync(() -> db.put(key.bytes(), val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> remove(@NotNull ByteArray key) {
        return runAsync(() -> db.delete(key.bytes()));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull Cursor<VaultEntry> range(@NotNull ByteArray fromKey, @NotNull ByteArray toKey) {
        try (var readOpts = new ReadOptions()) {
            var lowerBound = new Slice(fromKey.bytes());
            var upperBound = new Slice(toKey.bytes());

            readOpts
                .setIterateLowerBound(lowerBound)
                .setIterateUpperBound(upperBound);

            RocksIterator it = db.newIterator(readOpts);
            it.seekToFirst();

            return new RocksIteratorAdapter(it, lowerBound, upperBound);
        }
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> putAll(@NotNull Map<ByteArray, byte[]> vals) {
        return runAsync(() -> {
            try (
                var writeBatch = new WriteBatch();
                var writeOpts = new WriteOptions()
            ) {
                for (var entry : vals.entrySet()) {
                    if (entry.getValue() == null)
                        writeBatch.delete(entry.getKey().bytes());
                    else
                        writeBatch.put(entry.getKey().bytes(), entry.getValue());
                }

                db.write(writeOpts, writeBatch);
            }
        });
    }

    /**
     * Same as a {@link Supplier} but throws the {@link RocksDBException}.
     */
    @FunctionalInterface
    private static interface RocksSupplier<T> {
        /** */
        T supply() throws RocksDBException;
    }

    /**
     * Executes the given {@code supplier} on the internal thread pool.
     */
    private <T> CompletableFuture<T> supplyAsync(RocksSupplier<T> supplier) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return supplier.supply();
            } catch (RocksDBException e) {
                throw new IgniteInternalException(e);
            }
        }, threadPool);
    }

    /**
     * Same as a {@link Runnable} but throws the {@link RocksDBException}.
     */
    @FunctionalInterface
    private static interface RocksRunnable {
        /** */
        void run() throws RocksDBException;
    }

    /**
     * Executes the given {@code runnable} on the internal thread pool.
     */
    private CompletableFuture<Void> runAsync(RocksRunnable runnable) {
        return CompletableFuture.runAsync(() -> {
            try {
                runnable.run();
            } catch (RocksDBException e) {
                throw new IgniteInternalException(e);
            }
        }, threadPool);
    }
}
