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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.FilteredWalIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.util.lang.IgniteThrowableSupplier;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordPurpose.MIXED;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordPurpose.PHYSICAL;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.WalFilters.checkpoint;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.WalFilters.pageOwner;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.WalFilters.partitionMetaStateUpdate;

/**
 * Scanning WAL by specific condition.
 */
public class WalScanner {
    /** Low level WAL iterator. */
    private final IgniteThrowableSupplier<WALIterator> walIteratorSupplier;

    /**
     * @param preconfiguredIter Preconfgured iterator.
     * @param parametersBuilder Parameters for iterator.
     * @param factory Factory of iterator.
     */
    WalScanner(
        WALIterator preconfiguredIter,
        IteratorParametersBuilder parametersBuilder,
        IgniteWalIteratorFactory factory
    ) {
        if (preconfiguredIter != null)
            walIteratorSupplier = () -> preconfiguredIter;
        else
            walIteratorSupplier = () -> standaloneWalIterator(
                factory == null ? new IgniteWalIteratorFactory() : factory,
                parametersBuilder
            );
    }

    /**
     * @param iteratorFactory Factory of iterator.
     * @param parametersBuilder Parameters for iterator.
     * @return Standalone WAL iterator created by given parameters.
     * @throws IgniteCheckedException If failed.
     */
    private static WALIterator standaloneWalIterator(
        IgniteWalIteratorFactory iteratorFactory,
        IteratorParametersBuilder parametersBuilder
    ) throws IgniteCheckedException {
        return iteratorFactory.iterator(
            parametersBuilder.copy().addFilter((type, pointer) ->
                // PHYSICAL need fo page shanpshot or delta record.
                // MIXED need for partiton meta state update.
                type.purpose() == PHYSICAL || type.purpose() == MIXED
            )
        );
    }

    /**
     * Finding all page physical records whose pageId is contained in given collection.
     *
     * @param groupAndPageIds Search pages.
     * @return Final step for execution some action on result.
     */
    @NotNull public WalScanner.ScanTerminateStep findAllRecordsFor(
        @NotNull Collection<T2<Integer, Long>> groupAndPageIds
    ) {
        requireNonNull(groupAndPageIds);

        HashSet<T2<Integer, Long>> groupAndPageIds0 = new HashSet<>(groupAndPageIds);

        // Collect all (group, partition) partition pairs.
        Set<T2<Integer, Integer>> groupAndParts = groupAndPageIds0.stream()
            .map((tup) -> new T2<>(tup.get1(), PageIdUtils.partId(tup.get2())))
            .collect(Collectors.toSet());

        // Build WAL filter. (Checkoint, Page, Partition meta)
        Predicate<IgniteBiTuple<WALPointer, WALRecord>> filter = checkpoint()
            .or(pageOwner(groupAndPageIds0))
            .or(partitionMetaStateUpdate(groupAndParts));

        return new ScanTerminateStep(() -> new FilteredWalIterator(walIteratorSupplier.get(), filter));
    }

    /**
     * Factory method of {@link WalScanner}.
     *
     * @param walIterator Preconfigured WAL iterator.
     * @return Instance of {@link WalScanner}.
     */
    public static WalScanner buildWalScanner(WALIterator walIterator) {
        return new WalScanner(walIterator, null, null);
    }

    /**
     * Factory method of {@link WalScanner}.
     *
     * @param parametersBuilder Iterator parameters for customization.
     * @return Instance of {@link WalScanner}.
     */
    public static WalScanner buildWalScanner(IteratorParametersBuilder parametersBuilder) {
        return buildWalScanner(parametersBuilder, null);
    }

    /**
     * Factory method of {@link WalScanner}.
     *
     * @param parametersBuilder Iterator parameters for customization.
     * @param factory Custom instance of {@link IgniteWalIteratorFactory}.
     * @return Instance of {@link WalScanner}.
     */
    public static WalScanner buildWalScanner(
        IteratorParametersBuilder parametersBuilder,
        IgniteWalIteratorFactory factory
    ) {
        return new WalScanner(null, parametersBuilder, factory);
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
