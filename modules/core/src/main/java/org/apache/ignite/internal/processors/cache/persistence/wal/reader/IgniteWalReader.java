/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal.reader;

import java.io.File;
import java.io.FileWriter;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.java.JavaLogger;

/**
 * Example utility for WAL archive reader
 */
public class IgniteWalReader {
    public static void main(String[] args) {
        System.exit(runWalReader(args));
    }

    private static int runWalReader(String[] args) {
        try {
            if (F.isEmpty(args))
                return help();

            if (args.length < 2) {
                System.err.println("Missing required parameters");
                help();
                return 1;
            }

            final int pageSize = Integer.parseInt(args[0]);
            final String folder = args[1];
            final File walFilesFolder = new File(folder);
            if (!walFilesFolder.exists()) {
                System.err.println("Can't find folder with WAL segments [" + walFilesFolder + "]");
                return 2;
            }

            final JavaLogger log = new JavaLogger();
            final File dumpFile = new File("wal.dump.txt");
            int cnt = 0;
            final IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log, pageSize);
            try (WALIterator iter = factory.iterator(walFilesFolder)) {
                try (FileWriter writer = new FileWriter(dumpFile)) {
                    final String endl = String.format("%n");
                    while (iter.hasNextX()) {
                        final IgniteBiTuple<WALPointer, WALRecord> next = iter.nextX();
                        writer.write(next.get2().toString() + endl);
                        cnt++;
                    }
                }
            }
            System.out.println("WAL records [" + cnt + "] dumped to file [" + dumpFile.getAbsolutePath() + "]");
            return 0;
        }
        catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    private static int help() {
        System.out.println("Usage:\n");
        System.out.println(" walReader.[bat|sh] {pageSize} {walArchiveDir}");
        System.out.println("   {pageSize} - page size selected in grid memory configuration. Page size must be between 1kB and 16kB.");
        System.out.println("   {walArchiveDir} - WAL archive directory");
        return 0;
    }
}