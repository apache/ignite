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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 *
 */
public class CheckpointRecoveryFileStorage {
    /** Checkpoint recovery file name pattern. */
    public static final Pattern FILE_NAME_PATTERN = Pattern.compile("(\\d+)-(.*)-RECOVERY-(\\d+)\\.bin");

    /** Context. */
    private final GridKernalContext ctx;

    /**
     * File IO factory.
     */
    private final FileIOFactory fileIoFactory;

    /**
     * Recovery file location.
     */
    private final File dir;

    /**
     * Ctor.
     */
    public CheckpointRecoveryFileStorage(GridKernalContext ctx, File dir, FileIOFactory fileIoFactory) throws StorageException {
        this.ctx = ctx;
        this.dir = dir;
        this.fileIoFactory = fileIoFactory;

        if (!U.mkdirs(dir))
            throw new StorageException("Failed to create directory for checkpoint recovery files [dir=" + dir + ']');

    }

    /** */
    private static String fileName(long cpTs, UUID cpId, int idx) {
        return cpTs + "-" + cpId + "-" + "RECOVERY-" + idx + ".bin";
    }

    /**
     * Factory method.
     */
    public CheckpointRecoveryFile create(long cpTs, UUID cpId, int idx) throws StorageException {
        File file = new File(dir, fileName(cpTs, cpId, idx));

        try {
            FileIO fileIO = fileIoFactory.create(file, CREATE, TRUNCATE_EXISTING, WRITE);

            return new CheckpointRecoveryFile(ctx, cpTs, cpId, idx, file, fileIO);
        }
        catch (IOException e) {
            throw new StorageException("Failed to create checkpoint recovery file [file=" + file + ']', e);
        }
    }

    /**
     * Gets list of recovery files, satisfying given predicate (or all files in storage if predicate is null).
     *
     * @return List of recovery files.
     */
    public List<CheckpointRecoveryFile> list(@Nullable Predicate<UUID> predicate) throws StorageException {
        File[] files = dir.listFiles(f -> f.isFile() && f.getName().contains("-RECOVERY-"));
        List<CheckpointRecoveryFile> fileList = new ArrayList<>();

        for (File file : files) {
            Matcher matcher = FILE_NAME_PATTERN.matcher(file.getName());
            if (matcher.matches()) {
                long ts = Long.parseLong(matcher.group(1));
                UUID id = UUID.fromString(matcher.group(2));
                int idx = Integer.parseInt(matcher.group(3));

                if (predicate == null || predicate.test(id)) {
                    try {
                        fileList.add(new CheckpointRecoveryFile(ctx, ts, id, idx, file, fileIoFactory.create(file, READ)));
                    }
                    catch (IOException e) {
                        throw new StorageException("Failed to open checkpoint recovery file [file=" + file + ']', e);
                    }
                }
            }
        }

        return fileList;
    }

    /**
     * Deletes all recovery files in storage.
     */
    public void clear() throws StorageException {
        File[] files = dir.listFiles(f -> f.isFile()
            && f.getName().contains("-RECOVERY-")
            && FILE_NAME_PATTERN.matcher(f.getName()).matches());

        for (File file : files) {
            if (!file.delete())
                throw new StorageException("Failed to checkpoint recovery file [file=" + file + ']');
        }
    }
}
