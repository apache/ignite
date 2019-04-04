package org.apache.ignite.util;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccLeafIO;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.logger.NullLogger;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.Arrays.asList;

/**
 * WAL reader.
 */
public class WalReader {

    private static final String PAGE_SIZE = "pageSize";
    private static final String BINARY_METADATA_FILE_STORE_DIR = "binaryMetadataFileStoreDir";
    private static final String MARSHALLER_MAPPING_FILE_STORE_DIR = "marshallerMappingFileStoreDir";
    private static final String KEEP_BINARY = "keepBinary";
    private static final String WAL_DIR = "walDir";
    private static final String WAL_ARCHIVE_DIR = "walArchiveDir";
    private static final String RECORD_TYPES = "recordTypes";
    private static final String WAL_TIME_FROM_MILLIS = "walTimeFromMillis";
    private static final String WAL_TIME_TO_MILLIS = "walTimeToMillis";
    private static final String RECORD_CONTAINS_TEXT = "recordContainsText";

    public static void main(String[] args) throws Exception {
        final Options options = new Options();

        options.addOption(PAGE_SIZE, true,
            "Size of pages, which was selected for file store (1024, 2048, 4096, etc).");
        options.addOption(BINARY_METADATA_FILE_STORE_DIR, true,
            "Path to binary metadata dir.");
        options.addOption(MARSHALLER_MAPPING_FILE_STORE_DIR, true,
            "Path to marshaller dir.");
        options.addOption(KEEP_BINARY, false,
            "Keep binary flag.");
        options.addOption(WAL_DIR, true,
            "Path to dir with wal files.");
        options.addOption(WAL_ARCHIVE_DIR, true,
            "Path to dir with archive wal files.");
        options.addOption(RECORD_TYPES, true,
            "Comma-separated WAL record types (TX_RECORD, DATA_RECORD, etc).");
        options.addOption(WAL_TIME_FROM_MILLIS, true,
            "The start time interval for the last modification of files in milliseconds.");
        options.addOption(WAL_TIME_TO_MILLIS, true,
            "The end time interval for the last modification of files in milliseconds.");
        options.addOption(RECORD_CONTAINS_TEXT, true,
            "Filter by substring in the WAL record.");

        final CommandLine cmd = new DefaultParser().parse(options, args);

        final int pageSize;
        final File binaryMetadataFileStoreDir;
        final File marshallerMappingFileStoreDir;
        final boolean keepBinary;
        final String walPath;
        final String walArchivePath;
        final Set<String> recordTypes;
        final Long fromTime;
        final Long toTime;
        final String recordContainsText;

        final File[] walFiles;
        final File[] walArchiveFiles;

        try {

            pageSize = cmd.hasOption(PAGE_SIZE) ? parseInt(cmd.getOptionValue(PAGE_SIZE)) : 4096;
            String binaryMetadataFileStoreDirPath = cmd.getOptionValue(BINARY_METADATA_FILE_STORE_DIR, null);
            binaryMetadataFileStoreDir = !F.isEmpty(binaryMetadataFileStoreDirPath)
                ? new File(binaryMetadataFileStoreDirPath)
                : null;
            String marshallerMappingFileStoreDirPath = cmd.getOptionValue(MARSHALLER_MAPPING_FILE_STORE_DIR, null);
            marshallerMappingFileStoreDir = !F.isEmpty(marshallerMappingFileStoreDirPath)
                ? new File(marshallerMappingFileStoreDirPath)
                : null;
            keepBinary = cmd.hasOption(KEEP_BINARY);
            walPath = cmd.getOptionValue(WAL_DIR, null);
            walArchivePath = cmd.getOptionValue(WAL_ARCHIVE_DIR, null);
            recordTypes = cmd.hasOption(RECORD_TYPES) ? new HashSet<>(Arrays.asList(
                cmd.getOptionValue(RECORD_TYPES, null).split(",\\s*"))) : null;
            fromTime = cmd.hasOption(WAL_TIME_FROM_MILLIS) ? parseLong(cmd.getOptionValue(WAL_TIME_FROM_MILLIS)) : null;
            toTime = cmd.hasOption(WAL_TIME_TO_MILLIS) ? parseLong(cmd.getOptionValue(WAL_TIME_TO_MILLIS)) : null;
            recordContainsText = cmd.getOptionValue(RECORD_CONTAINS_TEXT, null);

            if (binaryMetadataFileStoreDir != null &&
                (!binaryMetadataFileStoreDir.exists() || !binaryMetadataFileStoreDir.isDirectory())) {
                throw new IllegalArgumentException(String.format(
                    "Binary metadata dir '%s' not found.", binaryMetadataFileStoreDir));
            }

            if (marshallerMappingFileStoreDir != null &&
                (!marshallerMappingFileStoreDir.exists() || !marshallerMappingFileStoreDir.isDirectory())) {
                throw new IllegalArgumentException(String.format(
                    "Marshaller dir '%s' not found.", binaryMetadataFileStoreDir));
            }

            if (F.isEmpty(walPath) && F.isEmpty(walArchivePath))
                throw new IllegalArgumentException("The paths to the WAL files are not specified.");

            if (!F.isEmpty(recordTypes)) {
                final SortedSet<String> unknownRecordTypes = new TreeSet<>();

                for (String recordType : recordTypes) {
                    try {
                        // Attempt to parse the record type.
                        WALRecord.RecordType.valueOf(recordType);
                    }
                    catch (Exception e) {
                        unknownRecordTypes.add(recordType);
                    }
                }

                if (!F.isEmpty(unknownRecordTypes)) {
                    final SortedSet<String> supportedRecordTypes = new TreeSet<>();

                    for (WALRecord.RecordType recordType : WALRecord.RecordType.values())
                        supportedRecordTypes.add(recordType.name());

                    throw new IllegalArgumentException("Unknown record types: " + unknownRecordTypes +
                        ". Supported record types: " + supportedRecordTypes);
                }
            }

            System.out.println("Program arguments:");
            System.out.printf("\t%s = %d%n", PAGE_SIZE, pageSize);
            if (binaryMetadataFileStoreDir != null)
                System.out.printf("\t%s = %s%n", BINARY_METADATA_FILE_STORE_DIR, binaryMetadataFileStoreDir);
            if (marshallerMappingFileStoreDir != null)
                System.out.printf("\t%s = %s%n", MARSHALLER_MAPPING_FILE_STORE_DIR, marshallerMappingFileStoreDir);
            System.out.printf("\t%s = %s%n", KEEP_BINARY, keepBinary);
            if (!F.isEmpty(walPath))
                System.out.printf("\t%s = %s%n", WAL_DIR, walPath);
            if (!F.isEmpty(walArchivePath))
                System.out.printf("\t%s = %s%n", WAL_ARCHIVE_DIR, walArchivePath);
            if (!F.isEmpty(recordTypes))
                System.out.printf("\t%s = %s%n", RECORD_TYPES, recordTypes);
            if (fromTime != null)
                System.out.printf("\t%s = %s%n", WAL_TIME_FROM_MILLIS, new Date(fromTime));
            if (toTime != null)
                System.out.printf("\t%s = %s%n", WAL_TIME_TO_MILLIS, new Date(toTime));
            if (!F.isEmpty(recordContainsText))
                System.out.printf("\t%s = %s%n", RECORD_CONTAINS_TEXT, recordContainsText);

            walFiles = !F.isEmpty(walPath) ? files(walPath, fromTime, toTime) : null;
            walArchiveFiles = !F.isEmpty(walArchivePath) ? files(walArchivePath, fromTime, toTime) : null;
        }
        catch (Exception e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setSyntaxPrefix("Usage: ");
            formatter.setWidth(-1);

            formatter.printHelp("wal-reader", options, true);

            throw e;
        }

        PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS, H2MvccInnerIO.VERSIONS, H2MvccLeafIO.VERSIONS);
        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();

        System.setProperty(IgniteSystemProperties.IGNITE_TO_STRING_MAX_LENGTH, String.valueOf(Integer.MAX_VALUE));

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(new NullLogger());

        if (walArchiveFiles != null) {
            try (WALIterator it = factory.iterator(
                new IgniteWalIteratorFactory.IteratorParametersBuilder().
                    pageSize(pageSize).
                    binaryMetadataFileStoreDir(binaryMetadataFileStoreDir).
                    marshallerMappingFileStoreDir(marshallerMappingFileStoreDir).
                    keepBinary(keepBinary).
                    filesOrDirs(walFiles).
                    filesOrDirs(walArchiveFiles))) {
                while (it.hasNextX())
                    printRecord(it.nextX().get2(), recordTypes, recordContainsText, false);
            }
        }
    }

    /**
     * @param path WAL directory path.
     * @param fromTime The start time of the last modification of the files.
     * @param toTime The end time of the last modification of the files.
     * @return WAL files.
     */
    private static File[] files(String path, Long fromTime, Long toTime) {
        final File dir = new File(path);

        if (!dir.exists() || !dir.isDirectory())
            throw new IllegalArgumentException("Incorrect directory path: " + path);

        final File[] files = dir.listFiles(new FileFilter() {
            @Override public boolean accept(File f) {
                if (!FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER.accept(f))
                    return false;

                long fileLastModifiedTime = getFileLastModifiedTime(f);

                if (fromTime != null && fileLastModifiedTime < fromTime)
                    return false;

                if (toTime != null && fileLastModifiedTime > toTime)
                    return false;

                return true;
            }
        });

        if (files != null) {
            Arrays.sort(files);

            for (File file : files) {
                try {
                    System.out.printf("'%s': %s%n", file, new Date(getFileLastModifiedTime(file)));
                }
                catch (Exception e) {
                    System.err.printf("Failed to get last modified time of file '%s'%n", file);
                }
            }
        }
        else
            System.out.printf("'%s' is empty%n", path);

        return files;
    }

    /**
     * @param f WAL file.
     * @return The last modification time of file.
     */
    private static long getFileLastModifiedTime(File f) {
        try {
            return Files.getLastModifiedTime(f.toPath()).toMillis();
        }
        catch (IOException e) {
            System.err.printf("Failed to get file '%s' modification time%n", f);

            return -1;
        }
    }

    /**
     * @param record WAL record.
     * @param types WAL record types.
     * @param recordContainsText Filter by substring in the WAL record.
     * @param archive Archive flag.
     */
    private static void printRecord(WALRecord record, Set<String> types, String recordContainsText, boolean archive) {
        if (F.isEmpty(types) || types.contains(record.type().name())) {
            try {
                String recordStr = record.toString();

                if (F.isEmpty(recordContainsText) || recordStr.contains(recordContainsText))
                    System.out.printf("[%s] %s%n", archive ? "A" : "W", recordStr);
            }
            catch (Exception e) {
                System.err.printf("Failed to print record (type=%s)%n", record.type());
            }
        }
    }
}
