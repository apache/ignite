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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.record.CacheState;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.nio.file.StandardOpenOption.READ;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.TMP_FILE_MATCHER;

/**
 * Abstraction responsible for managing checkpoint markers storage.
 */
public class CheckpointMarkersStorage {
    /** Checkpoint file name pattern. */
    public static final Pattern CP_FILE_NAME_PATTERN = Pattern.compile("(\\d+)-(.*)-(START|END)\\.bin");

    /** Logger. */
    protected IgniteLogger log;

    /** Checkpoint history. */
    private CheckpointHistory cpHistory;

    /** File I/O factory for writing checkpoint markers. */
    private final FileIOFactory ioFactory;

    /** Checkpoint metadata directory ("cp"), contains files with checkpoint start and end */
    public final File cpDir;

    /** Temporary write buffer. */
    private final ByteBuffer tmpWriteBuf;

    /**
     * @param logger Ignite logger.
     * @param history Checkpoint history.
     * @param factory IO factory.
     * @param absoluteWorkDir Directory path to checkpoint markers folder.
     * @throws IgniteCheckedException if fail.
     */
    CheckpointMarkersStorage(
        Function<Class<?>, IgniteLogger> logger,
        CheckpointHistory history,
        FileIOFactory factory,
        String absoluteWorkDir
    ) throws IgniteCheckedException {
        this.log = logger.apply(getClass());
        cpHistory = history;
        ioFactory = factory;

        cpDir = Paths.get(absoluteWorkDir, "cp").toFile();

        if (!U.mkdirs(cpDir))
            throw new IgniteCheckedException("Could not create directory for checkpoint metadata: " + cpDir);

        //File index + offset + length.
        tmpWriteBuf = ByteBuffer.allocateDirect(Long.BYTES + Integer.BYTES + Integer.BYTES);

        tmpWriteBuf.order(ByteOrder.nativeOrder());

    }

    /**
     * Cleanup checkpoint directory from all temporary files.
     */
    public void cleanupTempCheckpointDirectory() throws IgniteCheckedException {
        try {
            try (DirectoryStream<Path> files = Files.newDirectoryStream(cpDir.toPath(), TMP_FILE_MATCHER::matches)) {
                for (Path path : files)
                    Files.delete(path);
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to cleanup checkpoint directory from temporary files: " + cpDir, e);
        }
    }

    /**
     * Cleanup checkpoint directory from all temporary files.
     */
    public void cleanupCheckpointDirectory() throws IgniteCheckedException {
        if (cpHistory != null)
            cpHistory.clear();

        try {
            try (DirectoryStream<Path> files = Files.newDirectoryStream(cpDir.toPath())) {
                for (Path path : files)
                    Files.delete(path);
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to cleanup checkpoint directory: " + cpDir, e);
        }
    }

    /**
     * Filling internal structures with data from disk.
     */
    public void initialize() throws IgniteCheckedException {
        cpHistory.initialize(retrieveHistory());
    }

    /**
     * Wal truncate callback.
     *
     * @param highBound Upper bound.
     * @throws IgniteCheckedException If failed.
     */
    public void removeCheckpointsUntil(@Nullable WALPointer highBound) throws IgniteCheckedException {
        List<CheckpointEntry> rmvFromHist = history().onWalTruncated(highBound);

        for (CheckpointEntry cp : rmvFromHist)
            removeCheckpointFiles(cp);
    }

    /**
     * Logs and clears checkpoint history after checkpoint finish.
     *
     * @param chp Finished checkpoint.
     * @throws IgniteCheckedException If failed.
     */
    public void onCheckpointFinished(Checkpoint chp) throws IgniteCheckedException {
        List<CheckpointEntry> rmvFromHist = history().onCheckpointFinished(chp);

        for (CheckpointEntry cp : rmvFromHist)
            removeCheckpointFiles(cp);
    }

    /**
     * @return Read checkpoint status.
     * @throws IgniteCheckedException If failed to read checkpoint status page.
     */
    @SuppressWarnings("TooBroadScope")
    public CheckpointStatus readCheckpointStatus() throws IgniteCheckedException {
        long lastStartTs = 0;
        long lastEndTs = 0;

        UUID startId = CheckpointStatus.NULL_UUID;
        UUID endId = CheckpointStatus.NULL_UUID;

        File startFile = null;
        File endFile = null;

        WALPointer startPtr = CheckpointStatus.NULL_PTR;
        WALPointer endPtr = CheckpointStatus.NULL_PTR;

        File dir = cpDir;

        if (!dir.exists()) {
            log.warning("Read checkpoint status: checkpoint directory is not found.");

            return new CheckpointStatus(0, startId, startPtr, endId, endPtr);
        }

        File[] files = dir.listFiles();

        for (File file : files) {
            Matcher matcher = CP_FILE_NAME_PATTERN.matcher(file.getName());

            if (matcher.matches()) {
                long ts = Long.parseLong(matcher.group(1));
                UUID id = UUID.fromString(matcher.group(2));
                CheckpointEntryType type = CheckpointEntryType.valueOf(matcher.group(3));

                if (type == CheckpointEntryType.START && ts > lastStartTs) {
                    lastStartTs = ts;
                    startId = id;
                    startFile = file;
                }
                else if (type == CheckpointEntryType.END && ts > lastEndTs) {
                    lastEndTs = ts;
                    endId = id;
                    endFile = file;
                }
            }
        }

        ByteBuffer buf = ByteBuffer.allocate(WALPointer.POINTER_SIZE);
        buf.order(ByteOrder.nativeOrder());

        if (startFile != null)
            startPtr = readPointer(startFile, buf);

        if (endFile != null)
            endPtr = readPointer(endFile, buf);

        if (log.isInfoEnabled())
            log.info("Read checkpoint status [startMarker=" + startFile + ", endMarker=" + endFile + ']');

        return new CheckpointStatus(lastStartTs, startId, startPtr, endId, endPtr);
    }

    /**
     * Retreives checkpoint history form specified {@code dir}.
     *
     * @return List of checkpoints.
     */
    private List<CheckpointEntry> retrieveHistory() throws IgniteCheckedException {
        if (!cpDir.exists())
            return Collections.emptyList();

        try (DirectoryStream<Path> cpFiles = Files.newDirectoryStream(
            cpDir.toPath(),
            path -> CP_FILE_NAME_PATTERN.matcher(path.toFile().getName()).matches())
        ) {
            List<CheckpointEntry> checkpoints = new ArrayList<>();

            ByteBuffer buf = ByteBuffer.allocate(WALPointer.POINTER_SIZE);
            buf.order(ByteOrder.nativeOrder());

            for (Path cpFile : cpFiles) {
                CheckpointEntry cp = parseFromFile(buf, cpFile.toFile());

                if (cp != null)
                    checkpoints.add(cp);
            }

            return checkpoints;
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to load checkpoint history.", e);
        }
    }

    /**
     * Parses checkpoint entry from given file.
     *
     * @param buf Temporary byte buffer.
     * @param file Checkpoint file.
     */
    @Nullable private CheckpointEntry parseFromFile(ByteBuffer buf, File file) throws IgniteCheckedException {
        Matcher matcher = CP_FILE_NAME_PATTERN.matcher(file.getName());

        if (!matcher.matches())
            return null;

        CheckpointEntryType type = CheckpointEntryType.valueOf(matcher.group(3));

        if (type != CheckpointEntryType.START)
            return null;

        long cpTs = Long.parseLong(matcher.group(1));
        UUID cpId = UUID.fromString(matcher.group(2));

        WALPointer ptr = readPointer(file, buf);

        return createCheckPointEntry(cpTs, ptr, cpId, null, CheckpointEntryType.START);
    }

    /**
     * Loads WAL pointer from CP file
     *
     * @param cpMarkerFile Checkpoint mark file.
     * @return WAL pointer.
     * @throws IgniteCheckedException If failed to read mignite-put-get-exampleark file.
     */
    private WALPointer readPointer(File cpMarkerFile, ByteBuffer buf) throws IgniteCheckedException {
        buf.position(0);

        try (FileIO io = ioFactory.create(cpMarkerFile, READ)) {
            io.readFully(buf);

            buf.flip();

            return new WALPointer(buf.getLong(), buf.getInt(), buf.getInt());
        }
        catch (Exception e) {
            throw new IgniteCheckedException(
                "Failed to read checkpoint pointer from marker file: " + cpMarkerFile.getAbsolutePath(), e);
        }
    }

    /**
     * @param cpTs Checkpoint timestamp.
     * @param ptr Wal pointer of checkpoint.
     * @param cpId Checkpoint ID.
     * @param rec Checkpoint record.
     * @param type Checkpoint type.
     * @return Checkpoint entry.
     */
    private CheckpointEntry createCheckPointEntry(
        long cpTs,
        WALPointer ptr,
        UUID cpId,
        @Nullable CheckpointRecord rec,
        CheckpointEntryType type
    ) {
        assert cpTs > 0;
        assert ptr != null;
        assert cpId != null;
        assert type != null;

        Map<Integer, CacheState> cacheGrpStates = null;

        if (rec != null)
            cacheGrpStates = rec.cacheGroupStates();

        return new CheckpointEntry(cpTs, ptr, cpId, cacheGrpStates);
    }

    /**
     * Removes checkpoint start/end files belongs to given {@code cpEntry}.
     *
     * @param cpEntry Checkpoint entry.
     * @throws IgniteCheckedException If failed to delete.
     */
    private void removeCheckpointFiles(CheckpointEntry cpEntry) throws IgniteCheckedException {
        Path startFile = new File(cpDir.getAbsolutePath(), checkpointFileName(cpEntry, CheckpointEntryType.START)).toPath();
        Path endFile = new File(cpDir.getAbsolutePath(), checkpointFileName(cpEntry, CheckpointEntryType.END)).toPath();

        try {
            if (Files.exists(startFile))
                Files.delete(startFile);

            if (Files.exists(endFile))
                Files.delete(endFile);
        }
        catch (IOException e) {
            throw new StorageException("Failed to delete stale checkpoint files: " + cpEntry, e);
        }
    }

    /**
     * @param entryBuf Buffer which would be written to disk.
     * @param cp Prepared checkpoint entry.
     * @param type Type of checkpoint marker.
     * @param skipSync {@code true} if file sync should be skip after write.
     * @throws StorageException if fail.
     */
    private void writeCheckpointEntry(
        ByteBuffer entryBuf,
        CheckpointEntry cp,
        CheckpointEntryType type,
        boolean skipSync
    ) throws StorageException {
        String fileName = checkpointFileName(cp, type);
        String tmpFileName = fileName + FilePageStoreManager.TMP_SUFFIX;

        try {
            try (FileIO io = ioFactory.create(Paths.get(cpDir.getAbsolutePath(), skipSync ? fileName : tmpFileName).toFile(),
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {

                io.writeFully(entryBuf);

                entryBuf.clear();

                if (!skipSync)
                    io.force(true);
            }

            if (!skipSync)
                Files.move(Paths.get(cpDir.getAbsolutePath(), tmpFileName), Paths.get(cpDir.getAbsolutePath(), fileName));
        }
        catch (IOException e) {
            throw new StorageException("Failed to write checkpoint entry [ptr=" + cp.checkpointMark()
                + ", cpTs=" + cp.timestamp()
                + ", cpId=" + cp.checkpointId()
                + ", type=" + type + "]", e);
        }
    }

    /**
     * Writes checkpoint entry buffer {@code entryBuf} to specified checkpoint file with 2-phase protocol.
     *
     * @param cpTs Checkpoint timestamp.
     * @param cpId Checkpoint id.
     * @param ptr WAL pointer containing record.
     * @param rec Checkpoint WAL record.
     * @param type Checkpoint type.
     * @return Checkpoint entry which represents current checkpoint by given parameters.
     * @throws StorageException If failed to write checkpoint entry.
     */
    public CheckpointEntry writeCheckpointEntry(
        long cpTs,
        UUID cpId,
        WALPointer ptr,
        @Nullable CheckpointRecord rec,
        CheckpointEntryType type,
        boolean skipSync
    ) throws StorageException {
        CheckpointEntry entry = prepareCheckpointEntry(
            tmpWriteBuf,
            cpTs,
            cpId,
            ptr,
            rec,
            type
        );

        if (type == CheckpointEntryType.START)
            cpHistory.addCheckpoint(entry, rec.cacheGroupStates());

        writeCheckpointEntry(tmpWriteBuf, entry, type, skipSync);

        return entry;
    }

    /**
     * Prepares checkpoint entry containing WAL pointer to checkpoint record. Writes into given {@code ptrBuf} WAL
     * pointer content.
     *
     * @param entryBuf Buffer to fill
     * @param cpTs Checkpoint timestamp.
     * @param cpId Checkpoint id.
     * @param ptr WAL pointer containing record.
     * @param rec Checkpoint WAL record.
     * @param type Checkpoint type.
     * @return Checkpoint entry.
     */
    private CheckpointEntry prepareCheckpointEntry(
        ByteBuffer entryBuf,
        long cpTs,
        UUID cpId,
        WALPointer ptr,
        @Nullable CheckpointRecord rec,
        CheckpointEntryType type
    ) {
        assert ptr != null;

        entryBuf.rewind();

        entryBuf.putLong(ptr.index());

        entryBuf.putInt(ptr.fileOffset());

        entryBuf.putInt(ptr.length());

        entryBuf.flip();

        return createCheckPointEntry(cpTs, ptr, cpId, rec, type);
    }

    /**
     * @param cpTs Checkpoint timestamp.
     * @param cpId Checkpoint ID.
     * @param type Checkpoint type.
     * @return Checkpoint file name.
     */
    private static String checkpointFileName(long cpTs, UUID cpId, CheckpointEntryType type) {
        return cpTs + "-" + cpId + "-" + type + ".bin";
    }

    /**
     * @param cp Checkpoint entry.
     * @param type Checkpoint type.
     * @return Checkpoint file name.
     */
    public static String checkpointFileName(CheckpointEntry cp, CheckpointEntryType type) {
        return checkpointFileName(cp.timestamp(), cp.checkpointId(), type);
    }

    /**
     * @return Cached checkpoint history.
     */
    public CheckpointHistory history() {
        return cpHistory;
    }
}
