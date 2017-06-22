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

package org.apache.ignite.internal.processors.cache.persistence.db.wal.reader;

import java.io.File;
import java.io.FileNotFoundException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.AbstractWalRecordsIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;

/**
 * WAL reader iterator, for creation in standalone WAL reader tool
 * Operates over one directory, does not provide start and end boundaries
 */
public class StandaloneWalRecordsIterator extends AbstractWalRecordsIterator {

    private final File walArchiveDir;

    public StandaloneWalRecordsIterator(File walArchiveDir,
        IgniteLogger log,
        @NotNull final GridCacheSharedContext sharedCtx) throws IgniteCheckedException {
        super(log,
            sharedCtx,
            new RecordV1Serializer(sharedCtx),
            2 * 1024 * 1024);
        this.walArchiveDir = walArchiveDir;
        init(walArchiveDir);


        advance();
    }

    private void init(File walArchiveDir) throws IgniteCheckedException {
        FileWriteAheadLogManager.FileDescriptor[] descs = loadFileDescriptors(walArchiveDir);
        curIdx = !F.isEmpty(descs) ? descs[0].getIdx() : 0;

        curIdx--;

        if (log.isDebugEnabled())
            log.debug("Initialized WAL cursor [curIdx=" + curIdx + ']');
    }

    protected void advanceSegment() throws IgniteCheckedException {
        FileWriteAheadLogManager.ReadFileHandle cur0 = curHandle;

        if (cur0 != null) {
            cur0.close();

            curHandle = null;
        }

        curIdx++;


        // curHandle.workDir is false
        FileWriteAheadLogManager.FileDescriptor fd = new FileWriteAheadLogManager.FileDescriptor(
            new File(walArchiveDir,
                FileWriteAheadLogManager.FileDescriptor.fileName(curIdx)));

        if (log.isDebugEnabled())
            log.debug("Reading next file [absIdx=" + curIdx + ", file=" + fd.getAbsolutePath() + ']');

        assert fd != null;

        try {
            curHandle = initReadHandle(fd, null);
        }
        catch (FileNotFoundException e) {
            throw new IgniteCheckedException("Missing WAL segment in the archive", e);
        }

        curRec = null;
    }
}
