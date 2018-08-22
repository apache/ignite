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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.NullLogger;
import org.jetbrains.annotations.Nullable;

/**
 * Print WAL log data in human-readable form.
 */
public class IgniteWalConverter {
    /**
     * @param args Args.
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2)
            throw new IllegalArgumentException("\nYou need to provide:\n" +
                    "\t1. Size of pages, which was selected for file store (1024, 2048, 4096, etc).\n" +
                    "\t2. Path to dir with wal files.\n" +
                    "\t3. (Optional) Path to dir with archive wal files.");

        PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS);
        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();

        boolean printRecords = IgniteSystemProperties.getBoolean("PRINT_RECORDS", false); //TODO read them from argumetns
        boolean printStat = IgniteSystemProperties.getBoolean("PRINT_STAT", true); //TODO read them from argumetns

        final IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(new NullLogger());

        final File walWorkDirWithConsistentId = new File(args[1]);

        final File[] workFiles = walWorkDirWithConsistentId.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER);

        if (workFiles == null)
            throw new IllegalArgumentException("No .wal files in dir: " + args[1]);

        @Nullable final WalStat stat = printStat ? new WalStat() : null;

        IgniteWalIteratorFactory.IteratorParametersBuilder iteratorParametersBuilder =
                new IgniteWalIteratorFactory.IteratorParametersBuilder().filesOrDirs(workFiles)
                    .pageSize(Integer.parseInt(args[0]));

        try (WALIterator stIt = factory.iterator(iteratorParametersBuilder)) {
            while (stIt.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> next = stIt.nextX();

                final WALPointer pointer = next.get1();
                final WALRecord record = next.get2();

                if (stat != null)
                    stat.registerRecord(record, pointer, true);

                if (printRecords)
                    System.out.println("[W] " + record);
            }
        }

        if (args.length >= 3) {
            final File walArchiveDirWithConsistentId = new File(args[2]);

            try (WALIterator stIt = factory.iterator(walArchiveDirWithConsistentId)) {
                while (stIt.hasNextX()) {
                    IgniteBiTuple<WALPointer, WALRecord> next = stIt.nextX();

                    final WALPointer pointer = next.get1();
                    final WALRecord record = next.get2();

                    if (stat != null)
                        stat.registerRecord(record, pointer, false);

                    if (printRecords)
                        System.out.println("[A] " + record);
                }
            }
        }

        System.err.flush();

        if (stat != null)
            System.out.println("Statistic collected:\n" + stat.toString());
    }
}
