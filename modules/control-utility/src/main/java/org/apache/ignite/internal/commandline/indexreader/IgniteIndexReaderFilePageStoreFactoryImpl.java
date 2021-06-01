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

package org.apache.ignite.internal.commandline.indexreader;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.lang.IgniteOutClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Factory {@link FilePageStore} for case of regular pds.
 */
public class IgniteIndexReaderFilePageStoreFactoryImpl implements IgniteIndexReaderFilePageStoreFactory {
    /** Directory with data(partitions and index). */
    private final File dir;

    /** {@link FilePageStore} factory by page store version. */
    private final FileVersionCheckingFactory storeFactory;

    /** Metrics updater. */
    private final LongAdderMetric allocationTracker = new LongAdderMetric("n", "d");

    /** Page size. */
    private final int pageSize;

    /** Partition count. */
    private final int partCnt;

    /**
     * Constructor.
     *
     * @param dir Directory with data(partitions and index).
     * @param pageSize Page size.
     * @param partCnt Partition count.
     * @param filePageStoreVer Page store version.
     */
    public IgniteIndexReaderFilePageStoreFactoryImpl(File dir, int pageSize, int partCnt, int filePageStoreVer) {
        this.dir = dir;
        this.pageSize = pageSize;
        this.partCnt = partCnt;

        storeFactory = new FileVersionCheckingFactory(
            new AsyncFileIOFactory(),
            new AsyncFileIOFactory(),
            () -> pageSize
        ) {
            /** {@inheritDoc} */
            @Override public int latestVersion() {
                return filePageStoreVer;
            }
        };
    }

    /** {@inheritDoc} */
    @Override @Nullable public FilePageStore createFilePageStore(int partId, byte type, Collection<Throwable> errors)
        throws IgniteCheckedException {
        File file = getFile(dir, partId, null);

        return !file.exists() ? null : (FilePageStore)storeFactory.createPageStore(type, file, allocationTracker::add);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer headerBuffer(byte type) throws IgniteCheckedException {
        int ver = storeFactory.latestVersion();

        FilePageStore store =
            (FilePageStore)storeFactory.createPageStore(type, (IgniteOutClosure<Path>) null, allocationTracker::add);

        return store.header(type, storeFactory.headerSize(ver));
    }

    /** {@inheritDoc} */
    @Override public int pageSize() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override public int partitionCount() {
        return partCnt;
    }
}
