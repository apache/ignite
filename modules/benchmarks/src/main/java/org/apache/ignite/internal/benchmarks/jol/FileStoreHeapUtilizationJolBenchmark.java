/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.benchmarks.jol;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.openjdk.jol.info.GraphLayout;

import static org.apache.ignite.configuration.DataStorageConfiguration.MIN_PAGE_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;

/**
 *
 */
public class FileStoreHeapUtilizationJolBenchmark {
    /** */
    private void benchmark() throws IgniteCheckedException {
        FilePageStoreFactory factory = new FileVersionCheckingFactory(
            new AsyncFileIOFactory(),
            new AsyncFileIOFactory(),
            new DataStorageConfiguration()
                .setPageSize(MIN_PAGE_SIZE)
        );

        List<PageStore> stores = new LinkedList<>();

        File workDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false);

        for (int i = 0; i < 10000; i++) {
            final int p = i;

            PageStore ps = factory.createPageStore(
                PageMemory.FLAG_DATA,
                () -> getPartitionFilePath(workDir, p),
                new LongAdderMetric("NO-OP", null)
            );

            ps.ensure();

            ps.write(0, ByteBuffer.allocate(256), 1, false);

            stores.add(ps);
        }

        System.gc();

        GraphLayout layout = GraphLayout.parseInstance(stores);

        System.out.println("heap usage: " + layout.totalSize());

        U.delete(workDir);
    }

    /** */
    private Path getPartitionFilePath(File cacheWorkDir, int partId) {
        return new File(cacheWorkDir, String.format(PART_FILE_TEMPLATE, partId)).toPath();
    }

    /** */
    public static void main(String[] args) throws Exception {
        new FileStoreHeapUtilizationJolBenchmark().benchmark();
    }
}
