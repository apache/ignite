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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import com.google.common.collect.Sets;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder.withIteratorParameters;
import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandlers.printToFile;
import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandlers.printToLog;
import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.WalScanner.buildWalScanner;

public class WalScannerTest extends GridCommonAbstractTest {

    /** Cache name. */
    private static final String CACHE_NAME = "cache0";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(1024L * 1024 * 1024)
                    .setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    @Test
    public void findAllRecordsFor() throws Exception {
        long pageId = generateData();
        String workDir = U.defaultWorkDirectory();

        File targetFile = Paths.get(workDir, "output.txt").toFile();

        buildWalScanner(withIteratorParameters().log(log).filesOrDirs(workDir))
            .findAllRecordsFor(Sets.newHashSet(pageId))
            .forEach(printToLog(log).andThen(printToFile(targetFile)));
    }

    public long generateData() throws Exception {
        Ignite ignite = startGrid("node0");

        ignite.cluster().active(true);

        ignite.createCache(CACHE_NAME);
        try (IgniteDataStreamer<Integer, Integer> st = ignite.dataStreamer(CACHE_NAME)) {
            st.allowOverwrite(true);

            for (int i = 0; i < 10_000; i++)
                st.addData(i, i);
        }

        stopAllGrids();

        List<FileWALPointer> wal = new ArrayList<>();

        String workDir = U.defaultWorkDirectory();

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory();

        try (WALIterator it = factory.iterator(
            new IgniteWalIteratorFactory.IteratorParametersBuilder()
                .filesOrDirs(workDir)
        )) {
            while (it.hasNext()) {
                WALRecord record = it.next().get2();

                if (record instanceof PageSnapshot) {
                    return ((PageSnapshot)record).fullPageId().pageId();
                }
            }
        }

        return 0;
    }
}