/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.storage.snapshot.local;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.raft.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import org.apache.ignite.raft.jraft.error.RetryAgainException;
import org.apache.ignite.raft.jraft.storage.SnapshotThrottle;
import org.apache.ignite.raft.jraft.storage.io.LocalDirReader;
import org.apache.ignite.raft.jraft.storage.snapshot.Snapshot;
import org.apache.ignite.raft.jraft.util.ByteBufferCollector;

/**
 * Snapshot file reader
 */
public class SnapshotFileReader extends LocalDirReader {

    private final SnapshotThrottle snapshotThrottle;
    private LocalSnapshotMetaTable metaTable;

    public SnapshotFileReader(String path, SnapshotThrottle snapshotThrottle) {
        super(path);
        this.snapshotThrottle = snapshotThrottle;
    }

    public LocalSnapshotMetaTable getMetaTable() {
        return this.metaTable;
    }

    public void setMetaTable(LocalSnapshotMetaTable metaTable) {
        this.metaTable = metaTable;
    }

    public boolean open() {
        final File file = new File(getPath());
        return file.exists();
    }

    @Override
    public int readFile(final ByteBufferCollector metaBufferCollector, final String fileName, final long offset,
        final long maxCount) throws IOException, RetryAgainException {
        // read the whole meta file.
        if (fileName.equals(Snapshot.JRAFT_SNAPSHOT_META_FILE)) {
            final ByteBuffer metaBuf = this.metaTable.saveToByteBufferAsRemote();
            // because bufRef will flip the buffer before using, so we must set the meta buffer position to it's limit.
            metaBuf.position(metaBuf.limit());
            metaBufferCollector.setBuffer(metaBuf);
            return EOF;
        }
        final LocalFileMeta fileMeta = this.metaTable.getFileMeta(fileName);
        if (fileMeta == null) {
            throw new FileNotFoundException("LocalFileMeta not found for " + fileName);
        }

        // go through throttle
        long newMaxCount = maxCount;
        if (this.snapshotThrottle != null) {
            newMaxCount = this.snapshotThrottle.throttledByThroughput(maxCount);
            if (newMaxCount < maxCount) {
                // if it's not allowed to read partly or it's allowed but
                // throughput is throttled to 0, try again.
                if (newMaxCount == 0) {
                    throw new RetryAgainException("readFile throttled by throughput");
                }
            }
        }

        return readFileWithMeta(metaBufferCollector, fileName, fileMeta, offset, newMaxCount);
    }
}
