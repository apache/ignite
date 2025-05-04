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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.CLIArgumentBuilder.argument;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.CLIArgumentBuilder.optionalArgument;

/**
 * Parameters for IgniteWalConverter with parsed and validated.
 */
public class IgniteWalConverterArguments {
    /** */
    private static final String ROOT_DIR = "--root";

    /** */
    private static final String FOLDER_NAME = "--folder-name";

    /** */
    private static final String PAGE_SIZE = "--page-size";

    /** */
    private static final String KEEP_BINARY = "--keep-binary";

    /** */
    private static final String RECORD_TYPES = "--record-types";

    /** */
    private static final String WAL_TIME_FROM_MILLIS = "--wal-time-from-millis";

    /** */
    private static final String WAL_TIME_TO_MILLIS = "--wal-time-to-millis";

    /** */
    private static final String RECORD_CONTAINS_TEXT = "--record-contains-text";

    /** */
    private static final String PROCESS_SENSITIVE_DATA = "--process-sensitive-data";

    /** */
    private static final String PRINT_STAT = "--print-stat";

    /** */
    private static final String SKIP_CRC = "--skip-crc";

    /** Argument "pages". */
    private static final String PAGES = "--pages";

    /** Record pattern for {@link #PAGES}. */
    private static final Pattern PAGE_ID_PATTERN = Pattern.compile("(-?\\d+):(-?\\d+)");

    /** Node file tree. */
    private final NodeFileTree ft;

    /** Size of pages, which was selected for file store (1024, 2048, 4096, etc). */
    private final int pageSize;

    /** Keep binary flag. */
    private final boolean keepBinary;

    /** WAL record types (TX_RECORD, DATA_RECORD, etc). */
    private final Set<WALRecord.RecordType> recordTypes;

    /** The start time interval for the record time in milliseconds. */
    private final Long fromTime;

    /** The end time interval for the record time in milliseconds. */
    private final Long toTime;

    /** Filter by substring in the WAL record. */
    private final String recordContainsText;

    /** Strategy for the processing of sensitive data (SHOW, HIDE, HASH, MD5). */
    private final ProcessSensitiveData procSensitiveData;

    /** Write summary statistics for WAL */
    private final boolean printStat;

    /** Skip CRC calculation/check flag */
    private final boolean skipCrc;

    /** Pages for searching in format grpId:pageId. */
    private final Collection<T2<Integer, Long>> pages;

    /**
     * Constructor.
     *
     * @param ft                            Node file tree.
     * @param pageSize                      Size of pages, which was selected for file store (1024, 2048, 4096, etc).
     * @param keepBinary                    Keep binary flag.
     * @param recordTypes                   WAL record types (TX_RECORD, DATA_RECORD, etc).
     * @param fromTime                      The start time interval for the record time in milliseconds.
     * @param toTime                        The end time interval for the record time in milliseconds.
     * @param recordContainsText            Filter by substring in the WAL record.
     * @param procSensitiveData          Strategy for the processing of sensitive data (SHOW, HIDE, HASH, MD5).
     * @param printStat                     Write summary statistics for WAL.
     * @param skipCrc                       Skip CRC calculation/check flag.
     * @param pages                         Pages for searching in format grpId:pageId.
     */
    public IgniteWalConverterArguments(NodeFileTree ft, int pageSize,
        boolean keepBinary,
        Set<WALRecord.RecordType> recordTypes, Long fromTime, Long toTime, String recordContainsText,
        ProcessSensitiveData procSensitiveData,
        boolean printStat, boolean skipCrc, Collection<T2<Integer, Long>> pages) {
        this.ft = ft;
        this.pageSize = pageSize;
        this.keepBinary = keepBinary;
        this.recordTypes = recordTypes;
        this.fromTime = fromTime;
        this.toTime = toTime;
        this.recordContainsText = recordContainsText;
        this.procSensitiveData = procSensitiveData;
        this.printStat = printStat;
        this.skipCrc = skipCrc;
        this.pages = pages;
    }

    /**
     * Node file tree.
     *
     * @return Node file tree.
     */
    public NodeFileTree getFileTree() {
        return ft;
    }

    /**
     * Size of pages, which was selected for file store (1024, 2048, 4096, etc).
     *
     * @return pageSize
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Keep binary flag.
     *
     * @return keepBina
     */
    public boolean isKeepBinary() {
        return keepBinary;
    }

    /**
     * WAL record types (TX_RECORD, DATA_RECORD, etc).
     *
     * @return recordTypes
     */
    public Set<WALRecord.RecordType> getRecordTypes() {
        return recordTypes;
    }

    /**
     * The start time interval for the record time in milliseconds.
     *
     * @return fromTime
     */
    public Long getFromTime() {
        return fromTime;
    }

    /**
     * The end time interval for the record time in milliseconds.
     *
     * @return toTime
     */
    public Long getToTime() {
        return toTime;
    }

    /**
     * Filter by substring in the WAL record.
     *
     * @return recordContainsText
     */
    public String getRecordContainsText() {
        return recordContainsText;
    }

    /**
     * Strategy for the processing of sensitive data (SHOW, HIDE, HASH, MD5).
     *
     * @return processSensitiveData
     */
    public ProcessSensitiveData getProcessSensitiveData() {
        return procSensitiveData;
    }

    /**
     * Write summary statistics for WAL.
     *
     * @return printStat
     */
    public boolean isPrintStat() {
        return printStat;
    }

    /**
     * Skip CRC calculation/check flag.
     *
     * @return skipCrc
     */
    public boolean isSkipCrc() {
        return skipCrc;
    }

    /**
     * Return pages for searching in format grpId:pageId.
     *
     * @return Pages.
     */
    public Collection<T2<Integer, Long>> getPages() {
        return pages;
    }

    /**
     * Parse command line arguments and return filled IgniteWalConverterArguments
     *
     * @param args Command line arguments.
     * @param out Out print stream.
     * @return IgniteWalConverterArguments.
     */
    public static IgniteWalConverterArguments parse(final PrintStream out, String... args) {
        CLIArgumentParser p = new CLIArgumentParser(
            Collections.emptyList(),
            asList(
                argument(ROOT_DIR, String.class)
                        .withUsage("Root pds directory.")
                        .build(),
                argument(FOLDER_NAME, String.class)
                        .withUsage("Node specific folderName.")
                        .build(),
                    optionalArgument(PAGE_SIZE, String.class)
                            .withUsage("Size of pages, which was selected for file store (1024, 2048, 4096, etc.).")
                            .withDefault("4096")
                            .build(),
                optionalArgument(KEEP_BINARY, String.class)
                    .withUsage("Keep binary flag")
                    .withDefault("true")
                    .build(),
                optionalArgument(RECORD_TYPES, String.class)
                        .withUsage("Comma-separated WAL record types (TX_RECORD, DATA_RECORD, etc.).")
                        .withDefault("all")
                        .build(),
                optionalArgument(WAL_TIME_FROM_MILLIS, String.class)
                        .withUsage("The start time interval for the record time in milliseconds.")
                        .build(),
                optionalArgument(WAL_TIME_TO_MILLIS, String.class)
                        .withUsage("The end time interval for the record time in milliseconds.")
                        .build(),
                optionalArgument(RECORD_CONTAINS_TEXT, String.class)
                        .withUsage("Filter by substring in the WAL record.")
                        .build(),
                optionalArgument(PROCESS_SENSITIVE_DATA, String.class)
                        .withUsage("Strategy for the processing of sensitive data (SHOW, HIDE, HASH, MD5)")
                        .withDefault("SHOW")
                        .build(),
                optionalArgument(PRINT_STAT, String.class)
                        .withUsage("Write summary statistics for WAL")
                        .withDefault("false")
                        .build(),
                optionalArgument(SKIP_CRC, String.class)
                        .withUsage("Skip CRC calculation/check flag.")
                        .withDefault("false")
                        .build(),
                optionalArgument(PAGES, String.class)
                        .withUsage("Comma-separated pages or path to file with pages on each line in grpId:pageId format.")
                        .build()
            ),
            null
        );

        if (args == null || args.length < 1) {
            out.println("Print WAL log data in human-readable form.");

            System.out.println(p.usage());

            out.println("For example:");
            out.println("    --root=ft"); // fixme
            out.println("    --folder-name="); // fixme
            out.println("    --page-size=4096");
            out.println("    --keep-binary=true");
            out.println("    --record-types=DataRecord,TxRecord");
            out.println("    --wal-time-from-millis=1575158400000");
            out.println("    --wal-time-to-millis=1577836740999");
            out.println("    --record-contains-text=search string");
            out.println("    --process-sensitive-data=SHOW");
            out.println("    --skip-crc=true");
            out.println("    --pages=123456:789456123,123456:789456124");
            return null;
        }

        NodeFileTree ft = null;
        File root = null;
        String folderName = null;
        int pageSize = 4096;
        boolean keepBinary = true;
        final Set<WALRecord.RecordType> recordTypes = new HashSet<>();
        Long fromTime = null;
        Long toTime = null;
        String recordContainsText = null;
        ProcessSensitiveData procSensitiveData = ProcessSensitiveData.SHOW;
        boolean printStat = false;
        boolean skipCrc = false;
        Collection<T2<Integer, Long>> pages = emptyList();

        for (String arg : args) {
            if (arg.startsWith(ROOT_DIR + "=")) {
                final String rootPath = arg.substring(ROOT_DIR.length() + 1);

                root = new File(rootPath);

                if (!root.exists())
                    throw new IllegalArgumentException("Incorrect path to the root dir: " + root);

                ft = ensureNodeStorageExists(root, folderName);
            }
            else if (arg.startsWith(FOLDER_NAME + "=")) {
                folderName = arg.substring(FOLDER_NAME.length() + 1);

                ft = ensureNodeStorageExists(root, folderName);
            }
            else if (arg.startsWith(PAGE_SIZE + "=")) {
                final String pageSizeStr = arg.substring(PAGE_SIZE.length() + 1);

                try {
                    pageSize = Integer.parseInt(pageSizeStr);
                }
                catch (Exception e) {
                    throw new IllegalArgumentException("Incorrect page size. Error parse: " + pageSizeStr);
                }
            }
            else if (arg.startsWith(KEEP_BINARY + "="))
                keepBinary = parseBoolean(KEEP_BINARY, arg.substring(KEEP_BINARY.length() + 1));

            else if (arg.startsWith(RECORD_TYPES + "=")) {
                final String recordTypesStr = arg.substring(RECORD_TYPES.length() + 1);

                final String[] recordTypesStrArr = recordTypesStr.split(",");

                final SortedSet<String> unknownRecordTypes = new TreeSet<>();

                for (String recordTypeStr : recordTypesStrArr) {
                    try {
                        recordTypes.add(WALRecord.RecordType.valueOf(recordTypeStr));
                    }
                    catch (Exception e) {
                        unknownRecordTypes.add(recordTypeStr);
                    }
                }

                if (!unknownRecordTypes.isEmpty())
                    throw new IllegalArgumentException("Unknown record types: " + unknownRecordTypes +
                        ". Supported record types: " + Arrays.toString(WALRecord.RecordType.values()));
            }
            else if (arg.startsWith(WAL_TIME_FROM_MILLIS + "=")) {
                final String fromTimeStr = arg.substring(WAL_TIME_FROM_MILLIS.length() + 1);

                try {
                    fromTime = Long.parseLong(fromTimeStr);
                }
                catch (Exception e) {
                    throw new IllegalArgumentException("Incorrect walTimeFromMillis. Error parse: " + fromTimeStr);
                }
            }
            else if (arg.startsWith(WAL_TIME_TO_MILLIS + "=")) {
                final String toTimeStr = arg.substring(WAL_TIME_TO_MILLIS.length() + 1);

                try {
                    toTime = Long.parseLong(toTimeStr);
                }
                catch (Exception e) {
                    throw new IllegalArgumentException("Incorrect walTimeToMillis. Error parse: " + toTimeStr);
                }
            }
            else if (arg.startsWith(RECORD_CONTAINS_TEXT + "="))
                recordContainsText = arg.substring(RECORD_CONTAINS_TEXT.length() + 1);

            else if (arg.startsWith(PROCESS_SENSITIVE_DATA + "=")) {
                final String procSensitiveDataStr = arg.substring(PROCESS_SENSITIVE_DATA.length() + 1);
                try {
                    procSensitiveData = ProcessSensitiveData.valueOf(procSensitiveDataStr);
                }
                catch (Exception e) {
                    throw new IllegalArgumentException("Unknown processSensitiveData: " + procSensitiveDataStr +
                        ". Supported: " + Arrays.toString(ProcessSensitiveData.values()));
                }
            }
            else if (arg.startsWith(PRINT_STAT + "="))
                printStat = parseBoolean(PRINT_STAT, arg.substring(PRINT_STAT.length() + 1));

            else if (arg.startsWith(SKIP_CRC + "="))
                skipCrc = parseBoolean(SKIP_CRC, arg.substring(SKIP_CRC.length() + 1));

            else if (arg.startsWith(PAGES + "=")) {
                String pagesStr = arg.replace(PAGES + "=", "");

                File pagesFile = new File(pagesStr);

                pages = pagesFile.exists() ? parsePageIds(pagesFile) : parsePageIds(pagesStr.split(","));
            }
        }

        if (ft == null)
            throw new IllegalArgumentException("The paths to the node files are not specified.");

        out.println("Program arguments:");

        out.printf("\t%s = %s\n", ROOT_DIR, root.getAbsolutePath());
        out.printf("\t%s = %s\n", FOLDER_NAME, folderName);
        out.printf("\t%s = %d\n", PAGE_SIZE, pageSize);
        out.printf("\t%s = %s\n", KEEP_BINARY, keepBinary);

        if (!F.isEmpty(recordTypes))
            out.printf("\t%s = %s\n", RECORD_TYPES, recordTypes);

        if (fromTime != null)
            out.printf("\t%s = %s\n", WAL_TIME_FROM_MILLIS, new Date(fromTime));

        if (toTime != null)
            out.printf("\t%s = %s\n", WAL_TIME_TO_MILLIS, new Date(toTime));

        if (recordContainsText != null)
            out.printf("\t%s = %s\n", RECORD_CONTAINS_TEXT, recordContainsText);

        out.printf("\t%s = %b\n", PRINT_STAT, printStat);

        out.printf("\t%s = %b\n", SKIP_CRC, skipCrc);

        if (!pages.isEmpty())
            out.printf("\t%s = %s\n", PAGES, pages);

        return new IgniteWalConverterArguments(ft, pageSize,
            keepBinary, recordTypes, fromTime, toTime, recordContainsText, procSensitiveData, printStat, skipCrc,
            pages);
    }

    /**
     * Creates and check {@link NodeFileTree} when both params specified.
     * @param root Root directory.
     * @param folderName Folder name.
     * @return Node file tree if both parameter specified, {@code null} otherwise.
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
     * Parses the string argument as a boolean.  The {@code boolean}
     * returned represents the value {@code true} if the string argument
     * is not {@code null} and is equal, ignoring case, to the string
     * {@code "true"}, returned value {@code false} if the string argument
     * is not {@code null} and is equal, ignoring case, to the string
     * {@code "false"}, else throw IllegalArgumentException<p>
     *
     * @param name parameter name of boolean type.
     * @param val the {@code String} containing the boolean representation to be parsed.
     * @return the boolean represented by the string argument
     *
     */
    private static boolean parseBoolean(String name, String val) {
        if (val == null)
            throw new IllegalArgumentException("Null value passed for flag " + name);

        if (val.equalsIgnoreCase(Boolean.TRUE.toString()))
            return true;
        else if (val.equalsIgnoreCase(Boolean.FALSE.toString()))
            return false;
        else
            throw new IllegalArgumentException("Incorrect flag " + name + ", valid value: true or false. Error parse: " + val);
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
}
