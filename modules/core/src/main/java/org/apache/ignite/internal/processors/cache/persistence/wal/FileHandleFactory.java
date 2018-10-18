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

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.util.function.Supplier;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class FileHandleFactory {

    private final boolean walFsyncWithDedicatedWorker =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_WAL_FSYNC_WITH_DEDICATED_WORKER, false);

    private final DataStorageConfiguration dsConf;

    public FileHandleFactory(DataStorageConfiguration conf) {
        dsConf = conf;
    }

    public FileHandleManager build(GridCacheSharedContext cctx, DataStorageMetricsImpl metrics, boolean mmap,
        ThreadLocal<WALPointer> lastWALPtr, RecordSerializer serializer, Supplier<FileWriteHandle> currentHandle) {
        if (dsConf.getWalMode() == WALMode.FSYNC && !walFsyncWithDedicatedWorker)
            return new FsyncFileHandleManagerImpl(
                cctx,
                metrics,
                lastWALPtr,
                serializer,
                currentHandle,
                dsConf.getWalMode(),
                dsConf.getWalSegmentSize(),
                dsConf.getWalFsyncDelayNanos(),
                dsConf.getWalThreadLocalBufferSize()
            );
        else
            return new FileHandleManagerImpl(
                cctx,
                metrics,
                mmap,
                lastWALPtr,
                serializer,
                currentHandle,
                dsConf.getWalMode(),
                dsConf.getWalBufferSize(),
                dsConf.getWalSegmentSize(),
                dsConf.getWalFsyncDelayNanos()
            );
    }
}
