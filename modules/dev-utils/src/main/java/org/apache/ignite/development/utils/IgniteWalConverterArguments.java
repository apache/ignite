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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyList;

/**
 * Parameters for IgniteWalConverter with parsed and validated.
 */
public class IgniteWalConverterArguments {
    /** */
    private static final String WAL_DIR = "walDir";

    /** */
    private static final String WAL_ARCHIVE_DIR = "walArchiveDir";

    /** */
    private static final String PAGE_SIZE = "pageSize";

    /** */
    private static final String BINARY_METADATA_FILE_STORE_DIR = "binaryMetadataFileStoreDir";

    /** */
    private static final String MARSHALLER_MAPPING_FILE_STORE_DIR = "marshallerMappingFileStoreDir";

    /** */
    private static final String KEEP_BINARY = "keepBinary";

    /** */
    private static final String RECORD_TYPES = "recordTypes";

    /** */
    private static final String WAL_TIME_FROM_MILLIS = "walTimeFromMillis";

    /** */
    private static final String WAL_TIME_TO_MILLIS = "walTimeToMillis";

    /** */
    private static final String RECORD_CONTAINS_TEXT = "recordContainsText";

    /** */
    private static final String PROCESS_SENSITIVE_DATA = "processSensitiveData";

    /** */
    private static final String PRINT_STAT = "printStat";

    /** */
    private static final String SKIP_CRC = "skipCrc";

    /** Argument "pages". */
    private static final String PAGES = "pages";

    /** Record pattern for {@link #PAGES}. */
    private static final Pattern PAGE_ID_PATTERN = Pattern.compile("(\\d+):(\\d+)");

    /** Path to dir with wal files. */
    private final File walDir;

    /** Path to dir with archive wal files. */
    private final File walArchiveDir;

    /** Size of pages, which was selected for file store (1024, 2048, 4096, etc). */
    private final int pageSize;

    /** Path to binary metadata dir. */
    private final File binaryMetadataFileStoreDir;

    /** Path to marshaller dir. */
    private final File marshallerMappingFileStoreDir;

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
    private final ProcessSensitiveData processSensitiveData;

    /** Write summary statistics for WAL */
    private final boolean printStat;

    /** Skip CRC calculation/check flag */
    private final boolean skipCrc;

    /** Pages for searching in format grpId:pageId. */
    private final Collection<T2<Integer, Long>> pages;

    /**
     * Constructor.
     *
     * @param walDir                        Path to dir with wal files.
     * @param walArchiveDir                 Path to dir with archive wal files.
     * @param pageSize                      Size of pages, which was selected for file store (1024, 2048, 4096, etc).
     * @param binaryMetadataFileStoreDir    Path to binary metadata dir.
     * @param marshallerMappingFileStoreDir Path to marshaller dir.
     * @param keepBinary                    Keep binary flag.
     * @param recordTypes                   WAL record types (TX_RECORD, DATA_RECORD, etc).
     * @param fromTime                      The start time interval for the record time in milliseconds.
     * @param toTime                        The end time interval for the record time in milliseconds.
     * @param recordContainsText            Filter by substring in the WAL record.
     * @param processSensitiveData          Strategy for the processing of sensitive data (SHOW, HIDE, HASH, MD5).
     * @param printStat                     Write summary statistics for WAL.
     * @param skipCrc                       Skip CRC calculation/check flag.
     * @param pages                         Pages for searching in format grpId:pageId.
     */
    public IgniteWalConverterArguments(File walDir, File walArchiveDir, int pageSize,
        File binaryMetadataFileStoreDir, File marshallerMappingFileStoreDir, boolean keepBinary,
        Set<WALRecord.RecordType> recordTypes, Long fromTime, Long toTime, String recordContainsText,
        ProcessSensitiveData processSensitiveData,
        boolean printStat, boolean skipCrc, Collection<T2<Integer, Long>> pages) {
        this.walDir = walDir;
        this.walArchiveDir = walArchiveDir;
        this.pageSize = pageSize;
        this.binaryMetadataFileStoreDir = binaryMetadataFileStoreDir;
        this.marshallerMappingFileStoreDir = marshallerMappingFileStoreDir;
        this.keepBinary = keepBinary;
        this.recordTypes = recordTypes;
        this.fromTime = fromTime;
        this.toTime = toTime;
        this.recordContainsText = recordContainsText;
        this.processSensitiveData = processSensitiveData;
        this.printStat = printStat;
        this.skipCrc = skipCrc;
        this.pages = pages;
    }

    /**
     * Path to dir with wal files.
     *
     * @return walDir
     */
    public File getWalDir() {
        return walDir;
    }

    /**
     * Path to dir with archive wal files.
     *
     * @return walArchiveDir
     */
    public File getWalArchiveDir() {
        return walArchiveDir;
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
     * Path to binary metadata dir.
     *
     * @return binaryMetadataFileStoreD
     */
    public File getBinaryMetadataFileStoreDir() {
        return binaryMetadataFileStoreDir;
    }

    /**
     * Path to marshaller dir.
     *
     * @return marshallerMappingFileStoreD
     */
    public File getMarshallerMappingFileStoreDir() {
        return marshallerMappingFileStoreDir;
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
        return processSensitiveData;
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
     * @return IgniteWalConverterArguments.
     */
    public static IgniteWalConverterArguments parse(final PrintStream out, String... args) {
        if (args == null || args.length < 1) {
            out.println("Print WAL log data in human-readable form.");
            out.println("You need to provide:");
            out.println("    walDir                           Path to dir with wal files.");
            out.println("    walArchiveDir                    " +
                "Path to dir with archive wal files. walDir or walArchiveDir must be specified.");
            out.println("    pageSize                         " +
                "Size of pages, which was selected for file store (1024, 2048, 4096, etc). Default 4096.");
            out.println("    binaryMetadataFileStoreDir       (Optional) Path to binary meta.");
            out.println("    marshallerMappingFileStoreDir    (Optional) Path to marshaller dir.");
            out.println("    keepBinary                       Keep binary flag. Default true.");
            out.println("    recordTypes                      " +
                "(Optional) Comma-separated WAL record types (TX_RECORD, DATA_RECORD, etc). Default all.");
            out.println("    walTimeFromMillis                (Optional) The start time interval for the record time in milliseconds.");
            out.println("    walTimeToMillis                  (Optional) The end time interval for the record time in milliseconds.");
            out.println("    recordContainsText               (Optional) Filter by substring in the WAL record.");
            out.println("    processSensitiveData             " +
                "(Optional) Strategy for the processing of sensitive data (SHOW, HIDE, HASH, MD5). Default SHOW.");
            out.println("    printStat                        Write summary statistics for WAL. Default false.");
            out.println("    skipCrc                          Skip CRC calculation/check flag. Default false.");
            out.println("    pages                            (Optional) Comma-separated pages or path to file with " +
                "pages on each line in grpId:pageId format.");
            out.println("For example:");
            out.println("    walDir=/work/db/wal");
            out.println("    walArchiveDir=/work/db/wal_archive");
            out.println("    pageSize=4096");
            out.println("    binaryMetadataFileStoreDir=/work/db/nodeId-consistentId");
            out.println("    marshallerMappingFileStoreDir=/work/db/marshaller");
            out.println("    keepBinary=true");
            out.println("    recordTypes=DataRecord,TxRecord");
            out.println("    walTimeFromMillis=1575158400000");
            out.println("    walTimeToMillis=1577836740999");
            out.println("    recordContainsText=search string");
            out.println("    processSensitiveData=SHOW");
            out.println("    skipCrc=true");
            out.println("    pages=123456:789456123,123456:789456124");
            return null;
        }

        File walDir = null;
        File walArchiveDir = null;
        int pageSize = 4096;
        File binaryMetadataFileStoreDir = null;
        File marshallerMappingFileStoreDir = null;
        boolean keepBinary = true;
        final Set<WALRecord.RecordType> recordTypes = new HashSet<>();
        Long fromTime = null;
        Long toTime = null;
        String recordContainsText = null;
        ProcessSensitiveData processSensitiveData = ProcessSensitiveData.SHOW;
        boolean printStat = false;
        boolean skipCrc = false;
        Collection<T2<Integer, Long>> pages = emptyList();

        for (String arg : args) {
            if (arg.startsWith(WAL_DIR + "=")) {
                final String walPath = arg.substring(WAL_DIR.length() + 1);

                walDir = new File(walPath);

                if (!walDir.exists())
                    throw new IllegalArgumentException("Incorrect path to dir with wal files: " + walPath);
            }
            else if (arg.startsWith(WAL_ARCHIVE_DIR + "=")) {
                final String walArchivePath = arg.substring(WAL_ARCHIVE_DIR.length() + 1);

                walArchiveDir = new File(walArchivePath);

                if (!walArchiveDir.exists())
                    throw new IllegalArgumentException("Incorrect path to dir with archive wal files: " + walArchivePath);
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
            else if (arg.startsWith(BINARY_METADATA_FILE_STORE_DIR + "=")) {
                final String binaryMetadataFileStorePath = arg.substring(BINARY_METADATA_FILE_STORE_DIR.length() + 1);

                binaryMetadataFileStoreDir = new File(binaryMetadataFileStorePath);

                if (!binaryMetadataFileStoreDir.isDirectory())
                    throw new IllegalArgumentException("Incorrect path to dir with binary meta files: " + binaryMetadataFileStorePath);
            }
            else if (arg.startsWith(MARSHALLER_MAPPING_FILE_STORE_DIR + "=")) {
                final String marshallerMappingFileStorePath = arg.substring(MARSHALLER_MAPPING_FILE_STORE_DIR.length() + 1);

                marshallerMappingFileStoreDir = new File(marshallerMappingFileStorePath);

                if (!marshallerMappingFileStoreDir.isDirectory())
                    throw new IllegalArgumentException("Incorrect path to dir with marshaller files: " + marshallerMappingFileStorePath);
            }
            else if (arg.startsWith(KEEP_BINARY + "=")) {
                keepBinary = parseBoolean(KEEP_BINARY, arg.substring(KEEP_BINARY.length() + 1));
            }
            else if (arg.startsWith(RECORD_TYPES + "=")) {
                final String recordTypesStr = arg.substring(RECORD_TYPES.length() + 1);

                final String[] recordTypesStrArray = recordTypesStr.split(",");

                final SortedSet<String> unknownRecordTypes = new TreeSet<>();

                for (String recordTypeStr : recordTypesStrArray) {
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
            else if (arg.startsWith(RECORD_CONTAINS_TEXT + "=")) {
                recordContainsText = arg.substring(RECORD_CONTAINS_TEXT.length() + 1);
            }
            else if (arg.startsWith(PROCESS_SENSITIVE_DATA + "=")) {
                final String processSensitiveDataStr = arg.substring(PROCESS_SENSITIVE_DATA.length() + 1);
                try {
                    processSensitiveData = ProcessSensitiveData.valueOf(processSensitiveDataStr);
                }
                catch (Exception e) {
                    throw new IllegalArgumentException("Unknown processSensitiveData: " + processSensitiveDataStr +
                        ". Supported: " + Arrays.toString(ProcessSensitiveData.values()));
                }
            }
            else if (arg.startsWith(PRINT_STAT + "=")) {
                printStat = parseBoolean(PRINT_STAT, arg.substring(PRINT_STAT.length() + 1));
            }
            else if (arg.startsWith(SKIP_CRC + "=")) {
                skipCrc = parseBoolean(SKIP_CRC, arg.substring(SKIP_CRC.length() + 1));
            }
            else if (arg.startsWith(PAGES + "=")) {
                String pagesStr = arg.replace(PAGES + "=", "");

                File pagesFile = new File(pagesStr);

                pages = pagesFile.exists() ? parsePageIds(pagesFile) : parsePageIds(pagesStr.split(","));
            }
        }

        if (walDir == null && walArchiveDir == null)
            throw new IllegalArgumentException("The paths to the WAL files are not specified.");

        out.println("Program arguments:");

        if (walDir != null)
            out.printf("\t%s = %s\n", WAL_DIR, walDir.getAbsolutePath());

        if (walArchiveDir != null)
            out.printf("\t%s = %s\n", WAL_ARCHIVE_DIR, walArchiveDir.getAbsolutePath());

        out.printf("\t%s = %d\n", PAGE_SIZE, pageSize);

        if (binaryMetadataFileStoreDir != null)
            out.printf("\t%s = %s\n", BINARY_METADATA_FILE_STORE_DIR, binaryMetadataFileStoreDir);

        if (marshallerMappingFileStoreDir != null)
            out.printf("\t%s = %s\n", MARSHALLER_MAPPING_FILE_STORE_DIR, marshallerMappingFileStoreDir);

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

        return new IgniteWalConverterArguments(walDir, walArchiveDir, pageSize,
            binaryMetadataFileStoreDir, marshallerMappingFileStoreDir,
            keepBinary, recordTypes, fromTime, toTime, recordContainsText, processSensitiveData, printStat, skipCrc,
            pages);
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
     * @param value the {@code String} containing the boolean representation to be parsed.
     * @return the boolean represented by the string argument
     *
     */
    private static boolean parseBoolean(String name, String value) {
        if (value == null)
            throw new IllegalArgumentException("Null value passed for flag " + name);

        if (value.equalsIgnoreCase(Boolean.TRUE.toString()))
            return true;
        else if (value.equalsIgnoreCase(Boolean.FALSE.toString()))
            return false;
        else
            throw new IllegalArgumentException("Incorrect flag " + name + ", valid value: true or false. Error parse: " + value);
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
