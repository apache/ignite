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

package org.apache.ignite.development.utils;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.TimeStampRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.FilteredWalIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.WalFilters.checkpoint;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.WalFilters.pageOwner;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.WalFilters.partitionMetaStateUpdate;

/**
 * Print WAL log data in human-readable form.
 */
public class IgniteWalConverter {
    /**
     * @param args Args.
     * @throws Exception If failed.
     */
    public static void main(String[] args) {
        final IgniteWalConverterArguments parameters = IgniteWalConverterArguments.parse(System.out, args);

        if (parameters != null)
            convert(System.out, parameters);
    }

    /**
     * Write to out WAL log data in human-readable form.
     *
     * @param out        Receiver of result.
     * @param params Parameters.
     */
    public static void convert(final PrintStream out, final IgniteWalConverterArguments params) {
        System.setProperty(IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE,
            Boolean.toString(params.getProcessSensitiveData() == ProcessSensitiveData.HIDE));

        System.setProperty(IgniteSystemProperties.IGNITE_PDS_SKIP_CRC, Boolean.toString(params.isSkipCrc()));
        RecordV1Serializer.skipCrc = params.isSkipCrc();

        System.setProperty(IgniteSystemProperties.IGNITE_TO_STRING_MAX_LENGTH, String.valueOf(Integer.MAX_VALUE));

        final WalStat stat = params.isPrintStat() ? new WalStat() : null;

        IgniteWalIteratorFactory.IteratorParametersBuilder iteratorParametersBuilder =
            new IgniteWalIteratorFactory.IteratorParametersBuilder()
                .pageSize(params.getPageSize())
                .binaryMetadataFileStoreDir(params.getBinaryMetadataFileStoreDir())
                .marshallerMappingFileStoreDir(params.getMarshallerMappingFileStoreDir())
                .keepBinary(params.isKeepBinary());

        if (params.getWalDir() != null)
            iteratorParametersBuilder.filesOrDirs(params.getWalDir());

        if (params.getWalArchiveDir() != null)
            iteratorParametersBuilder.filesOrDirs(params.getWalArchiveDir());

        final IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory();

        boolean printAlways = F.isEmpty(params.getRecordTypes());

        try (WALIterator stIt = walIterator(factory.iterator(iteratorParametersBuilder), params.getPages())) {
            String currentWalPath = null;

            while (stIt.hasNextX()) {
                final String currentRecordWalPath = getCurrentWalFilePath(stIt);

                if (currentWalPath == null || !currentWalPath.equals(currentRecordWalPath)) {
                    out.println("File: " + currentRecordWalPath);

                    currentWalPath = currentRecordWalPath;
                }

                IgniteBiTuple<WALPointer, WALRecord> next = stIt.nextX();

                final WALPointer pointer = next.get1();

                final WALRecord record = next.get2();

                if (stat != null)
                    stat.registerRecord(record, pointer, true);

                if (printAlways || params.getRecordTypes().contains(record.type())) {
                    boolean print = true;

                    if (record instanceof TimeStampRecord)
                        print = withinTimeRange((TimeStampRecord) record, params.getFromTime(), params.getToTime());

                    final String recordStr = toString(record, params.getProcessSensitiveData());

                    if (print && (F.isEmpty(params.getRecordContainsText()) || recordStr.contains(params.getRecordContainsText())))
                        out.println(recordStr);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace(out);
        }

        if (stat != null)
            out.println("Statistic collected:\n" + stat.toString());
    }

    /**
     * Checks if provided TimeStampRecord is within time range.
     *
     * @param rec Record.
     * @param fromTime Lower bound for timestamp.
     * @param toTime Upper bound for timestamp;
     * @return {@code True} if timestamp is within range.
     */
    private static boolean withinTimeRange(TimeStampRecord rec, Long fromTime, Long toTime) {
        if (fromTime != null && rec.timestamp() < fromTime)
            return false;

        if (toTime != null && rec.timestamp() > toTime)
            return false;

        return true;
    }

    /**
     * Get current wal file path, used in {@code WALIterator}.
     *
     * @param it WALIterator.
     * @return Current wal file path.
     */
    private static String getCurrentWalFilePath(WALIterator it) {
        String res = null;

        try {
            WALIterator walIter = it instanceof FilteredWalIterator ? U.field(it, "delegateWalIter") : it;

            Integer curIdx = U.field(walIter, "curIdx");

            List<FileDescriptor> walFileDescriptors = U.field(walIter, "walFileDescriptors");

            if (curIdx != null && walFileDescriptors != null && curIdx < walFileDescriptors.size())
                res = walFileDescriptors.get(curIdx).getAbsolutePath();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return res;
    }

    /**
     * Converting {@link WALRecord} to a string with sensitive data.
     *
     * @param walRecord     Instance of {@link WALRecord}.
     * @param sensitiveData Strategy for processing of sensitive data.
     * @return String representation of {@link WALRecord}.
     */
    private static String toString(WALRecord walRecord, ProcessSensitiveData sensitiveData) {
        if (walRecord instanceof DataRecord) {
            final DataRecord dataRecord = (DataRecord)walRecord;

            final List<DataEntry> entryWrappers = new ArrayList<>(dataRecord.writeEntries().size());

            for (DataEntry dataEntry : dataRecord.writeEntries())
                entryWrappers.add(new DataEntryWrapper(dataEntry, sensitiveData));

            dataRecord.setWriteEntries(entryWrappers);
        }
        else if (walRecord instanceof MetastoreDataRecord)
            walRecord = new MetastoreDataRecordWrapper((MetastoreDataRecord)walRecord, sensitiveData);

        return walRecord.toString();
    }

    /**
     * Getting WAL iterator.
     *
     * @param walIter WAL iterator.
     * @param pageIds Pages for searching in format grpId:pageId.
     * @return WAL iterator.
     */
    private static WALIterator walIterator(
        WALIterator walIter,
        Collection<T2<Integer, Long>> pageIds
    ) throws IgniteCheckedException {
        Predicate<IgniteBiTuple<WALPointer, WALRecord>> filter = null;

        if (!pageIds.isEmpty()) {
            Set<T2<Integer, Long>> grpAndPageIds0 = new HashSet<>(pageIds);

            // Collect all (group, partition) partition pairs.
            Set<T2<Integer, Integer>> grpAndParts = grpAndPageIds0.stream()
                .map((tup) -> new T2<>(tup.get1(), PageIdUtils.partId(tup.get2())))
                .collect(Collectors.toSet());

            // Build WAL filter. (Checkoint, Page, Partition meta)
            filter = checkpoint().or(pageOwner(grpAndPageIds0)).or(partitionMetaStateUpdate(grpAndParts));
        }

        return filter != null ? new FilteredWalIterator(walIter, filter) : walIter;
    }
}
