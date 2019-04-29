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

package org.apache.ignite.internal.processors.cache.persistence.wal.scanner;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.FilteredWalIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.lang.IgniteThrowableSupplier;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.WalFilters.checkpoint;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.WalFilters.pageOwner;

/**
 * Scanning WAL by specific condition.
 */
public class WalScanner {
    /** Parameters for iterator. */
    private final IgniteWalIteratorFactory.IteratorParametersBuilder parametersBuilder;
    /** Wal iterator factory. */
    private final IgniteWalIteratorFactory iteratorFactory;

    /**
     * @param parametersBuilder Parameters for iterator.
     * @param factory Factory of iterator.
     */
    WalScanner(IgniteWalIteratorFactory.IteratorParametersBuilder parametersBuilder,
        IgniteWalIteratorFactory factory) {
        this.parametersBuilder = parametersBuilder;
        iteratorFactory = factory == null ? new IgniteWalIteratorFactory() : factory;

        //Only physical records make sense.
        this.parametersBuilder.addFilter((type, pointer) -> type.purpose() == WALRecord.RecordPurpose.PHYSICAL);
    }

    /**
     * Finding all records whose pageId is contained in given collection.
     *
     * @param pageIds Search pages.
     * @return Final step for execution some action on result.
     */
    @NotNull public WalScanner.ScanTerminateStep findAllRecordsFor(@NotNull Collection<Long> pageIds) {
        requireNonNull(pageIds);

        return new ScanTerminateStep(() -> iterator(pageOwner(new HashSet<>(pageIds)).or(checkpoint())));
    }

    /**
     * @param filter Record filter.
     * @return Instance of {@link FilteredWalIterator}.
     * @throws IgniteCheckedException If initialization of iterator will be failed.
     */
    @NotNull private FilteredWalIterator iterator(
        Predicate<IgniteBiTuple<WALPointer, WALRecord>> filter
    ) throws IgniteCheckedException {
        return new FilteredWalIterator(iteratorFactory.iterator(parametersBuilder), filter);
    }

    /**
     * Factory method of {@link WalScanner}.
     *
     * @param parametersBuilder Iterator parameters for customization.
     * @return Instance of {@link WalScanner}.
     */
    public static WalScanner buildWalScanner(IgniteWalIteratorFactory.IteratorParametersBuilder parametersBuilder) {
        return new WalScanner(parametersBuilder, null);
    }

    /**
     * Factory method of {@link WalScanner}.
     *
     * @param parametersBuilder Iterator parameters for customization.
     * @return Instance of {@link WalScanner}.
     */
    public static WalScanner buildWalScanner(IgniteWalIteratorFactory.IteratorParametersBuilder parametersBuilder,
        IgniteWalIteratorFactory factory) {
        return new WalScanner(parametersBuilder, factory);
    }

    /**
     * @param filesOrDirs Paths to files or directories.
     * @return WalPageIteratorBuilder Self reference.
     */
    public static File[] asFiles(String... filesOrDirs) {
        File[] filesOrDirs0 = new File[filesOrDirs.length];

        for (int i = 0; i < filesOrDirs.length; i++)
            filesOrDirs0[i] = new File(filesOrDirs[i]);

        return filesOrDirs0;
    }

    /**
     * Terminate state of scanning of WAL for ability to do chaining flow.
     */
    public static class ScanTerminateStep {
        /** WAL iteration supplier. */
        final IgniteThrowableSupplier<WALIterator> iterSupplier;

        /**
         * @param iterSupplier WAL iteration supplier.
         */
        private ScanTerminateStep(IgniteThrowableSupplier<WALIterator> iterSupplier) {
            this.iterSupplier = iterSupplier;
        }

        /**
         * Execute given handler on each record.
         *
         * @param handler Single record handler.
         * @throws IgniteCheckedException If iteration was failed.
         */
        public void forEach(@NotNull ScannerHandler handler) throws IgniteCheckedException {
            try (WALIterator it = iterSupplier.get()) {
                while (it.hasNext())
                    handler.handle(it.next());
            }
            finally {
                handler.finish();
            }
        }
    }

}
