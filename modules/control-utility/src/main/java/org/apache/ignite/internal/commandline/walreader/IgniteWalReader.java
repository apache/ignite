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

package org.apache.ignite.internal.commandline.walreader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.TimeStampRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.FilteredWalIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.CLIArgumentBuilder.argument;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.CLIArgumentBuilder.optionalArgument;
import static org.apache.ignite.internal.commandline.walreader.ProcessSensitiveData.SHOW;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.WalFilters.checkpoint;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.WalFilters.pageOwner;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.WalFilters.partitionMetaStateUpdate;

/**
 * Print WAL log data in human-readable form.
 */
public class IgniteWalReader implements AutoCloseable {
    /**
     *
     */
    private static final String ROOT_DIR = "--root";

    /**
     *
     */
    private static final String FOLDER_NAME = "--folder-name";

    /**
     *
     */
    private static final String PAGE_SIZE = "--page-size";

    /**
     *
     */
    private static final String KEEP_BINARY = "--keep-binary";

    /**
     *
     */
    private static final String RECORD_TYPES = "--record-types";

    /**
     *
     */
    private static final String WAL_TIME_FROM_MILLIS = "--wal-time-from-millis";

    /**
     *
     */
    private static final String WAL_TIME_TO_MILLIS = "--wal-time-to-millis";

    /**
     *
     */
    private static final String RECORD_CONTAINS_TEXT = "--record-contains-text";

    /**
     *
     */
    private static final String PROCESS_SENSITIVE_DATA = "--process-sensitive-data";

    /**
     *
     */
    private static final String PRINT_STAT = "--print-stat";

    /**
     *
     */
    private static final String SKIP_CRC = "--skip-crc";

    /**
     * Argument "pages".
     */
    private static final String PAGES = "--pages";

    /**
     * Record pattern for {@link #PAGES}.
     */
    private static final Pattern PAGE_ID_PATTERN = Pattern.compile("(-?\\d+):(-?\\d+)");

    /**
     * Node file tree.
     */
    private final NodeFileTree ft;

    /**
     * Size of pages, which was selected for file store (1024, 2048, 4096, etc).
     */
    private final Integer pageSize;

    /**
     * Keep binary flag.
     */
    private final Boolean keepBinary;

    /**
     * WAL record types (TX_RECORD, DATA_RECORD, etc).
     */
    private final Set<WALRecord.RecordType> recordTypes;

    /**
     * The start time interval for the record time in milliseconds.
     */
    private final Long fromTime;

    /**
     * The end time interval for the record time in milliseconds.
     */
    private final Long toTime;

    /**
     * Filter by substring in the WAL record.
     */
    private final String recordContainsText;

    /**
     * Strategy for the processing of sensitive data (SHOW, HIDE, HASH, MD5).
     */
    private final ProcessSensitiveData procSensitiveData;

    /**
     * Write summary statistics for WAL
     */
    private final Boolean printStat;

    /**
     * Skip CRC calculation/check flag
     */
    private final Boolean skipCrc;

    /**
     * Pages for searching in format grpId:pageId.
     */
    private final Collection<T2<Integer, Long>> pages;

    /**
     * Logger.
     */
    private final IgniteLogger log;

    /**
     * @param ft               Node file tree.
     * @param pageSize         Size of pages, which was selected for file store (1024, 2048, 4096, etc).
     * @param keepBinary       Keep binary flag.
     * @param recordTypes      WAL record types (TX_RECORD, DATA_RECORD, etc).
     * @param fromTime         The start time interval for the record time in milliseconds.
     * @param toTime           The end time interval for the record time in milliseconds.
     * @param recordContainsText Filter by substring in the WAL record.
     * @param procSensitiveData Strategy for the processing of sensitive data (SHOW, HIDE, HASH, MD5).
     * @param printStat        Write summary statistics for WAL.
     * @param skipCrc          Skip CRC calculation/check flag.
     * @param pages            Pages for searching in format grpId:pageId.
     * @param log              Logger.
     */
    public IgniteWalReader(
            NodeFileTree ft,
            @Nullable Integer pageSize,
            @Nullable Boolean keepBinary,
            Set<WALRecord.RecordType> recordTypes,
            @Nullable Long fromTime,
            @Nullable Long toTime,
            @Nullable String recordContainsText,
            ProcessSensitiveData procSensitiveData,
            @Nullable Boolean printStat,
            @Nullable Boolean skipCrc,
            Collection<T2<Integer, Long>> pages,
            IgniteLogger log
    ) {
        this.ft = ft;
        this.pageSize = pageSize;
        this.keepBinary = keepBinary;
        this.recordTypes = new HashSet<>(recordTypes);
        this.fromTime = fromTime;
        this.toTime = toTime;
        this.recordContainsText = recordContainsText;
        this.procSensitiveData = procSensitiveData;
        this.printStat = printStat;
        this.skipCrc = skipCrc;
        this.pages = new ArrayList<>(pages);
        this.log = log;
    }

    /**
     * @param args Args.
     */
    public static void main(String[] args) {
        IgniteLogger log = CommandHandler.setupJavaLogger("wal-reader", IgniteWalReader.class);

        CLIArgumentParser p = new CLIArgumentParser(
            Collections.emptyList(),
            asList(
                argument(ROOT_DIR, String.class)
                    .withUsage("Root pds directory.")
                    .build(),
                argument(FOLDER_NAME, String.class)
                    .withUsage("Node specific folderName.")
                    .build(),
                optionalArgument(PAGE_SIZE, Integer.class)
                    .withUsage("Size of pages, which was selected for file store (1024, 2048, 4096, etc.).")
                    .withDefault(4096)
                    .build(),
                optionalArgument(KEEP_BINARY, Boolean.class)
                    .withUsage("Keep binary flag")
                    .withDefault(true)
                    .build(),
                optionalArgument(RECORD_TYPES, String.class)
                    .withUsage("Comma-separated WAL record types (TX_RECORD, DATA_RECORD, etc.).")
                    .build(),
                optionalArgument(WAL_TIME_FROM_MILLIS, Long.class)
                    .withUsage("The start time interval for the record time in milliseconds.")
                    .build(),
                optionalArgument(WAL_TIME_TO_MILLIS, Long.class)
                    .withUsage("The end time interval for the record time in milliseconds.")
                    .build(),
                optionalArgument(RECORD_CONTAINS_TEXT, String.class)
                    .withUsage("Filter by substring in the WAL record.")
                    .build(),
                optionalArgument(PROCESS_SENSITIVE_DATA, ProcessSensitiveData.class)
                    .withUsage("Strategy for the processing of sensitive data (SHOW, HIDE, HASH, MD5)")
                    .withDefault(SHOW)
                    .build(),
                optionalArgument(PRINT_STAT, Boolean.class)
                    .withUsage("Write summary statistics for WAL")
                    .withDefault(false)
                    .build(),
                optionalArgument(SKIP_CRC, Boolean.class)
                    .withUsage("Skip CRC calculation/check flag.")
                    .withDefault(false)
                    .build(),
                optionalArgument(PAGES, String.class)
                    .withUsage("Comma-separated pages or path to file with pages on each line in grpId:pageId format.")
                    .build()
                ),
                null
        );

        if (args.length == 0) {
            log.info(p.usage());

            return;
        }

        p.parse(asList(args).listIterator());

        File root = new File((String)p.get(ROOT_DIR));

        NodeFileTree ft = ensureNodeStorageExists(root, p.get(FOLDER_NAME));

        Collection<T2<Integer, Long>> pages = collectPages(p.get(PAGES));

        Set<WALRecord.RecordType> validRecordTypes = validateRecordTypes(p.get(RECORD_TYPES));

        try (IgniteWalReader reader = new IgniteWalReader(
                ft,
                p.get(PAGE_SIZE),
                p.get(KEEP_BINARY),
                validRecordTypes,
                p.get(WAL_TIME_FROM_MILLIS),
                p.get(WAL_TIME_TO_MILLIS),
                p.get(RECORD_CONTAINS_TEXT),
                p.get(PROCESS_SENSITIVE_DATA),
                p.get(PRINT_STAT),
                p.get(SKIP_CRC),
                pages,
                log
        )) {
            reader.read();
        }
    }

    /**
     * @param recordTypesStr Record types string.
     */
    static Set<WALRecord.RecordType> validateRecordTypes(String recordTypesStr) {
        if (recordTypesStr == null || recordTypesStr.isEmpty())
            return new HashSet<>();

        Set<WALRecord.RecordType> validRecordTypes = new HashSet<>();

        final String[] recordTypesStrArr = recordTypesStr.split(",");

        final SortedSet<String> unknownRecordTypes = new TreeSet<>();

        for (String recordTypeStr : recordTypesStrArr) {
            try {
                validRecordTypes.add(WALRecord.RecordType.valueOf(recordTypeStr));
            }
            catch (Exception e) {
                unknownRecordTypes.add(recordTypeStr);
            }
        }

        if (!unknownRecordTypes.isEmpty())
            throw new IllegalArgumentException("Unknown record types: " + unknownRecordTypes +
                    ". Supported record types: " + Arrays.toString(WALRecord.RecordType.values()));

        return validRecordTypes;
    }

    /**
     * @param root       Root.
     * @param folderName Folder name.
     */
    private static NodeFileTree ensureNodeStorageExists(@Nullable File root, @Nullable String folderName) {
        if (root == null || folderName == null)
            return null;

        NodeFileTree ft = new NodeFileTree(root, folderName);

        if (!ft.wal().exists() && !ft.walArchive().exists())
            throw new IllegalArgumentException("WAL directories not exists: " + ft.wal() + ", " + ft.walArchive());

        return ft;
    }

    /**
     * @param pagesStr Pages string.
     */
    static Collection<T2<Integer, Long>> collectPages(String pagesStr) {
        if (pagesStr == null || pagesStr.isEmpty())
            return emptyList();

        File pagesFile = new File(pagesStr);

        if (pagesFile.exists())
            return parsePageIds(pagesFile);
        else
            return parsePageIds(pagesStr.split(","));
    }

    /**
     * Parsing and checking the string representation of the page in grpId:pageId format.
     * Example: 123:456.
     *
     * @param s String value.
     * @return Parsed value.
     * @throws IllegalArgumentException If the string value is invalid.
     */
    static T2<Integer, Long> parsePageId(@Nullable String s) throws IllegalArgumentException {
        if (s == null)
            throw new IllegalArgumentException("Null value.");
        else if (s.isEmpty())
            throw new IllegalArgumentException("Empty value.");

        Matcher m = PAGE_ID_PATTERN.matcher(s);

        if (!m.matches()) {
            throw new IllegalArgumentException("Incorrect value " + s + ", valid format: grpId:pageId. " +
                "Example: 123:456");
        }

        return new T2<>(Integer.parseInt(m.group(1)), Long.parseLong(m.group(2)));
    }

    /**
     * Parsing a file in which each line is expected to be grpId:pageId format.
     *
     * @param f File.
     * @return Parsed pages.
     * @throws IllegalArgumentException If there is an error when working with a file or parsing lines.
     * @see #parsePageId
     */
    static Collection<T2<Integer, Long>> parsePageIds(File f) throws IllegalArgumentException {
        try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
            int i = 0;
            String s;

            Collection<T2<Integer, Long>> res = new ArrayList<>();

            while ((s = reader.readLine()) != null) {
                try {
                    res.add(parsePageId(s));
                }
                catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(
                        "Error parsing value \"" + s + "\" on " + i + " line of the file: " + f.getAbsolutePath(),
                        e
                    );
                }

                i++;
            }

            return res.isEmpty() ? emptyList() : res;
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Error when working with the file: " + f.getAbsolutePath(), e);
        }
    }

    /**
     * Parsing strings in which each element is expected to be in grpId:pageId format.
     *
     * @param strs String values.
     * @return Parsed pages.
     * @throws IllegalArgumentException If there is an error parsing the strs.
     * @see #parsePageId
     */
    static Collection<T2<Integer, Long>> parsePageIds(String... strs) throws IllegalArgumentException {
        Collection<T2<Integer, Long>> res = new ArrayList<>();

        for (int i = 0; i < strs.length; i++) {
            try {
                res.add(parsePageId(strs[i]));
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Error parsing value \"" + strs[i] + "\" of " + i + " element", e);
            }
        }

        return res.isEmpty() ? emptyList() : res;
    }

    /**
     * Write to out WAL log data in human-readable form.
     */
    public void read() {
        System.setProperty(IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE,
            Boolean.toString(procSensitiveData == ProcessSensitiveData.HIDE));

        System.setProperty(IgniteSystemProperties.IGNITE_PDS_SKIP_CRC, Boolean.toString(skipCrc));
        RecordV1Serializer.skipCrc = skipCrc;

        System.setProperty(IgniteSystemProperties.IGNITE_TO_STRING_MAX_LENGTH, String.valueOf(Integer.MAX_VALUE));

        final WalStat stat = printStat ? new WalStat() : null;

        IgniteWalIteratorFactory.IteratorParametersBuilder iterParametersBuilder =
            new IgniteWalIteratorFactory.IteratorParametersBuilder()
                .fileTree(ft)
                .pageSize(pageSize)
                .keepBinary(keepBinary);

        if (ft.wal().exists())
            iterParametersBuilder.filesOrDirs(ft.wal());

        if (ft.walArchive().exists())
            iterParametersBuilder.filesOrDirs(ft.walArchive());

        final IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory();

        boolean printAlways = F.isEmpty(recordTypes);

        try (WALIterator stIt = walIterator(factory.iterator(iterParametersBuilder), pages)) {
            String curWalPath = null;

            while (stIt.hasNextX()) {
                final String curRecordWalPath = getCurrentWalFilePath(stIt);

                if (curWalPath == null || !curWalPath.equals(curRecordWalPath)) {
                    log.info("File: " + curRecordWalPath);

                    curWalPath = curRecordWalPath;
                }

                IgniteBiTuple<WALPointer, WALRecord> next = stIt.nextX();

                final WALPointer pointer = next.get1();

                final WALRecord record = next.get2();

                if (stat != null)
                    stat.registerRecord(record, pointer, true);

                if (printAlways || recordTypes.contains(record.type())) {
                    boolean print = true;

                    if (record instanceof TimeStampRecord)
                        print = withinTimeRange((TimeStampRecord)record, fromTime, toTime);

                    final String recordStr = toString(record, procSensitiveData);

                    if (print && (F.isEmpty(recordContainsText) || recordStr.contains(recordContainsText)))
                        log.info(recordStr);
                }
            }
        }
        catch (IgniteCheckedException e) {
            log.warning("Getting wal iterator failed [grpId:pageId =" + pages + ']');
        }

        if (stat != null)
            log.info("Statistic collected:\n" + stat);
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
            throw new IgniteException("Failed to read current WAL file path", e);
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

            int entryCnt = dataRecord.entryCount();

            final List<DataEntry> entryWrappers = new ArrayList<>(entryCnt);

            for (int i = 0; i < entryCnt; i++)
                entryWrappers.add(new DataEntryWrapper(dataRecord.get(i), sensitiveData));

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

    /** {@inheritDoc} */
    @Override public void close() {
        // no-op
    }
}
