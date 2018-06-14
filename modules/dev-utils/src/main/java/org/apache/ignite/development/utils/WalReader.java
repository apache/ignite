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

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.Nullable;

/**
 * WAL Reader
 */
public class WalReader {

    private static void printUsage() {
        System.err.println("Usage: java -jar wal-reader.jar <PAGE_SIZE> <WORK_DIR> <WAL_ARCHIVE_PATH> <CONSISTENT_ID> <FROM_NAME_ID-TO_NAME_ID> <RECORD_TYPES (comma-separated)>");
        System.err.println("For example: java -jar wal-reader-2.4.2-p7.jar 4096 \"/opt/pprb/server/work/\" \"/gridgain/sas/wal_archive/10_126_0_164_47500/\" \"10_126_0_164_47500\" \"0000000000000001-0000000000001234\" \"TX_RECORD,DATA_RECORD\"");
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS);
        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();

        if (args.length != 5 && args.length != 6) {
            printUsage();
            return;
        }

        final Integer pageSize;
        final String workDir;
        final String walArchivePath;
        final String consistentId;
        final long nameFromId, nameToId;
        Set<String> recordTypes = null;

        try {

            pageSize = Integer.parseInt(args[0]);
            workDir = args[1];
            walArchivePath = args[2];
            consistentId = args[3];
            String[] fromTo = args[4].split("-");
            nameFromId = Long.parseLong(fromTo[0]);
            nameToId = Long.parseLong(fromTo[1]);
            if (args.length > 5)
                recordTypes = new HashSet<>(Arrays.asList(args[5].split(",")));


        }
        catch (Exception e) {
            printUsage();
            return;
        }

        final File binaryMeta = U.resolveWorkDirectory(workDir, "binary_meta", false);
        final File binaryMetaWithConsistentId = new File(binaryMeta, consistentId);
        final File marshallerMapping = null; // U.resolveWorkDirectory(workDir, "marshaller", false)

        final File walArchiveDirWithConsistentId = new File(walArchivePath, consistentId);

        if (!binaryMetaWithConsistentId.exists() || !binaryMetaWithConsistentId.isDirectory()) {
            IGNITE_LOGGER.error(String.format("Incorrect directory path: '%s'", binaryMetaWithConsistentId));
            return;
        }

        if (!walArchiveDirWithConsistentId.exists() || !walArchiveDirWithConsistentId.isDirectory()) {
            IGNITE_LOGGER.error(String.format("Incorrect directory path: '%s'", walArchiveDirWithConsistentId));
            return;
        }

        IGNITE_LOGGER.info(String.format("Work directory: '%s', binary meta: '%s'", workDir, binaryMetaWithConsistentId));

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(
            IGNITE_LOGGER,
            pageSize,
            binaryMetaWithConsistentId,
            marshallerMapping,
            false);

        File[] files = walArchiveDirWithConsistentId.listFiles(new FileFilter() {
            @Override public boolean accept(File file) {
                String name = file.getName();
                if (!file.isDirectory() && WAL_NAME_PATTERN.matcher(name).matches()) {
                    try {
                        long id = Long.parseLong(name.substring(0, name.indexOf(".")));
                        return id >= nameFromId && id <= nameToId;
                    }
                    catch (Exception ignore) {
                    }
                }
                return false;
            }
        });

        if (files != null) {
            Arrays.sort(files, new Comparator<File>() {
                @Override public int compare(File o1, File o2) {
                    return o1.getName().compareTo(o2.getName());
                }
            });

            IGNITE_LOGGER.info(String.format("Files for processing: %s", Arrays.toString(files)));

            try (WALIterator iter = factory.iteratorArchiveFiles(files)) {
                while (iter.hasNextX()) {
                    final IgniteBiTuple<WALPointer, WALRecord> next = iter.nextX();
                    final WALRecord walRecord = next.get2();

                    if (recordTypes == null || recordTypes.contains(walRecord.type().name())) {
                        try {
                            System.out.println(walRecord);
                        } catch (Exception e) {
                            IGNITE_LOGGER.error(String.format("Failed to print record (type=%s)", walRecord.type()));
                        }
                    }
                }
            }
        }
        else {
            IGNITE_LOGGER.error(String.format("Files in this directory '%s' are not found.", walArchiveDirWithConsistentId));
        }

    }

    private static final IgniteLogger IGNITE_LOGGER = new IgniteLogger() {

        private final Logger logger = LogManager.getLogger(WalReader.class);

        @Override public IgniteLogger getLogger(Object ctgr) {
            return null;
        }

        @Override public void trace(String msg) {
            logger.trace(msg);
        }

        @Override public void debug(String msg) {
            logger.debug(msg);
        }

        @Override public void info(String msg) {
            logger.info(msg);
        }

        @Override public void warning(String msg) {
            logger.warn(msg);
        }

        @Override public void warning(String msg, @Nullable Throwable e) {
            logger.warn(msg);
        }

        @Override public void error(String msg) {
            logger.error(msg);
        }

        @Override public void error(String msg, @Nullable Throwable e) {
            logger.error(msg);
        }

        @Override public boolean isTraceEnabled() {
            return false;
        }

        @Override public boolean isDebugEnabled() {
            return false;
        }

        @Override public boolean isInfoEnabled() {
            return false;
        }

        @Override public boolean isQuiet() {
            return false;
        }

        @Override public String fileName() {
            return null;
        }
    };

    /** Pattern for segment file names */
    private static final Pattern WAL_NAME_PATTERN = Pattern.compile("\\d{16}\\.wal");
}
