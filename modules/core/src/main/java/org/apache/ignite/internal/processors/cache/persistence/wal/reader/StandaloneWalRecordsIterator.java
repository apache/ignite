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
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.AbstractWalRecordsIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * WAL reader iterator, for creation in standalone WAL reader tool
 * Operates over one directory, does not provide start and end boundaries
 */
class StandaloneWalRecordsIterator extends AbstractWalRecordsIterator {
    /** */
    private static final long serialVersionUID = 0L;

    /** Record buffer size */
    private static final int BUF_SIZE = 2 * 1024 * 1024;

    /**
     * WAL files directory. Should already contain 'consistent ID' as subfolder.
     * <code>null</code> value means file-by-file iteration mode
     */
    @Nullable
    private File walFilesDir;

    /**
     * File descriptors remained to scan.
     * <code>null</code> value means directory scan mode
     */
    @Nullable
    private List<FileWriteAheadLogManager.FileDescriptor> walFileDescriptors;

    /**
     * Creates iterator in directory scan mode
     *
     * @param walFilesDir Wal files directory. Should already contain node consistent ID as subfolder
     * @param log Logger.
     * @param sharedCtx Shared context.
     */
    StandaloneWalRecordsIterator(
        @NotNull final File walFilesDir,
        @NotNull final IgniteLogger log,
        @NotNull final GridCacheSharedContext sharedCtx) throws IgniteCheckedException {
        super(log,
            sharedCtx,
            new RecordV1Serializer(sharedCtx),
            BUF_SIZE);
        init(walFilesDir, null);
        advance();
    }

    /**
     * Creates iterator in file-by-file iteration mode. Directory
     *
     * @param log Logger.
     * @param sharedCtx Shared context.
     * @param walFiles Wal files.
     */
    StandaloneWalRecordsIterator(
        @NotNull final IgniteLogger log,
        @NotNull final GridCacheSharedContext sharedCtx,
        @NotNull final File... walFiles) throws IgniteCheckedException {
        super(log,
            sharedCtx,
            new RecordV1Serializer(sharedCtx),
            BUF_SIZE);
        init(null, walFiles);
        advance();
    }

    /**
     * For directory mode sets oldest file as initial segment,
     * for file by file mode, converts all files to descriptors and gets oldest as initial.
     *
     * @param walFilesDir directory for directory scan mode
     * @param walFiles files for file-by-file iteration mode
     */
    private void init(@Nullable final File walFilesDir, @Nullable final File[] walFiles) throws IgniteCheckedException {
        if (walFilesDir != null) {
            FileWriteAheadLogManager.FileDescriptor[] descs = loadFileDescriptors(walFilesDir);
            curIdx = !F.isEmpty(descs) ? descs[0].getIdx() : 0;
            this.walFilesDir = walFilesDir;
        }
        else {
            FileWriteAheadLogManager.FileDescriptor[] descs = FileWriteAheadLogManager.scan(walFiles);
            walFileDescriptors = new ArrayList<>(Arrays.asList(descs));
            curIdx = !walFileDescriptors.isEmpty() ? walFileDescriptors.get(0).getIdx() : 0;
        }
        curIdx--;

        if (log.isDebugEnabled())
            log.debug("Initialized WAL cursor [curIdx=" + curIdx + ']');
    }

    /** {@inheritDoc} */
    @Override protected void advanceSegment() throws IgniteCheckedException {
        FileWriteAheadLogManager.ReadFileHandle cur0 = curHandle;

        if (cur0 != null) {
            cur0.close();

            curHandle = null;
        }

        curIdx++;
        // curHandle.workDir is false
        FileWriteAheadLogManager.FileDescriptor fd;
        if (walFilesDir != null) {
            fd = new FileWriteAheadLogManager.FileDescriptor(
                new File(walFilesDir,
                    FileWriteAheadLogManager.FileDescriptor.fileName(curIdx)));
        }
        else {
            if (walFileDescriptors.isEmpty()) {
                curHandle = null;
                return;
            }
            fd = walFileDescriptors.remove(0);
        }

        if (log.isDebugEnabled())
            log.debug("Reading next file [absIdx=" + curIdx + ", file=" + fd.getAbsolutePath() + ']');

        assert fd != null;

        try {
            curHandle = initReadHandle(fd, null);
        }
        catch (FileNotFoundException e) {
            log.info("Missing WAL segment in the archive" + e.getMessage());
            curHandle = null;
        }
        curRec = null;
    }

    /** {@inheritDoc} */
    @Override protected void handleRecordException(Exception e) {
        super.handleRecordException(e);
        e.printStackTrace();
    }

    /** {@inheritDoc} */
    @Override protected void onClose() throws IgniteCheckedException {
        super.onClose();
        curRec = null;

        if (curHandle != null) {
            curHandle.close();
            curHandle = null;
        }

        curIdx = Integer.MAX_VALUE;
    }
}
