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
package org.apache.ignite.internal.processors.cache.persistence.file;

import java.nio.file.Path;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.lang.IgniteOutClosure;

/**
 *
 */
public class FilePageStoreV2 extends FilePageStore {
    /** File version. */
    public static final int VERSION = 2;

    /** Header size. */
    private final int hdrSize;

    /**
     * Constructor which initializes file path provider closure, allowing to calculate file path in any time.
     *
     * @param type Type.
     * @param pathProvider file path provider.
     * @param factory Factory.
     * @param cfg Config.
     * @param allocatedTracker Allocated tracker.
     */
    public FilePageStoreV2(
        byte type,
        IgniteOutClosure<Path> pathProvider,
        FileIOFactory factory,
        DataStorageConfiguration cfg,
        LongAdderMetric allocatedTracker) {
        super(type, pathProvider, factory, cfg, allocatedTracker);

        hdrSize = cfg.getPageSize();
    }

    /** {@inheritDoc} */
    @Override public int headerSize() {
        return hdrSize;
    }

    /** {@inheritDoc} */
    @Override public int version() {
        return VERSION;
    }

    /** {@inheritDoc} */
    @Override public int getBlockSize() {
        return fileIO.getFileSystemBlockSize();
    }

    /** {@inheritDoc} */
    @Override public long getSparseSize() {
        FileIO io = fileIO;

        return io == null ? 0 : fileIO.getSparseSize();
    }

    /** {@inheritDoc} */
    @Override public void punchHole(long pageId, int usefulBytes) {
        assert usefulBytes >= 0 && usefulBytes < pageSize : usefulBytes;

        long off = pageOffset(pageId);

        fileIO.punchHole(off + usefulBytes, pageSize - usefulBytes);
    }
}
