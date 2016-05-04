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

package org.apache.ignite.internal.mem.file;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.mem.DirectMemory;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.lifecycle.LifecycleAware;

/**
 *
 */
public class MappedFileMemoryProvider implements DirectMemoryProvider, LifecycleAware {
    /** */
    private static final String ALLOCATOR_FILE_PREFIX = "allocator-";

    /** */
    private static final FilenameFilter ALLOCATOR_FILTER = new FilenameFilter() {
        @Override public boolean accept(File dir, String name) {
            return name.startsWith(ALLOCATOR_FILE_PREFIX);
        }
    };

    /** Logger to use. */
    private IgniteLogger log;

    /** File allocation path. */
    private final File allocationPath;

    /** Clean flag. If true, existing files will be deleted on start. */
    private final boolean clean;

    /** Allocation limit. */
    private final long limit;

    /** Allocation chunk size. */
    private final long chunkSize;

    /** */
    private boolean restored;

    /** */
    private List<MappedFile> mappedFiles;

    /**
     * @param allocationPath Allocation path.
     * @param clean Clean flag. If true, restore procedure will be ignored even if
     *      allocation folder contains valid files.
     * @param limit Allocation limit.
     */
    public MappedFileMemoryProvider(IgniteLogger log, File allocationPath, boolean clean, long limit, long chunkSize) {
        this.log = log;
        this.allocationPath = allocationPath;
        this.clean = clean;
        this.limit = limit;
        this.chunkSize = chunkSize;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        if (!allocationPath.exists()) {
            if (!allocationPath.mkdirs())
                throw new IgniteException("Failed to initialize allocation path (make sure directory is " +
                    "writable for the current user): " + allocationPath);
        }

        if (!allocationPath.isDirectory())
            throw new IgniteException("Failed to initialize allocation path (path is a file): " + allocationPath);

        File[] files = allocationPath.listFiles(ALLOCATOR_FILTER);

        if (files.length == 0 || clean) {
            if (files.length != 0) {
                log.info("Will clean up the following files upon start: " + Arrays.asList(files));

                for (File file : files) {
                    if (!file.delete())
                        throw new IgniteException("Failed to delete allocated file on start (make sure file is not " +
                            "opened by another process and current user has enough rights): " + file);
                }
            }

            allocateClean();

            return;
        }

        log.info("Restoring memory state from the files: " + Arrays.asList(files));

        mappedFiles = new ArrayList<>(files.length);

        try {
            for (File file : files) {
                MappedFile mapped = new MappedFile(file, 0);

                mappedFiles.add(mapped);
            }
        }
        catch (IOException e) {
            // Close all files allocated so far.
            try {
                for (MappedFile mapped : mappedFiles)
                    mapped.close();
            }
            catch (IOException e0) {
                e.addSuppressed(e0);
            }

            throw new IgniteException(e);
        }

        restored = true;
    }

    /**
     * Allocates clear memory state.
     */
    private void allocateClean() {
        mappedFiles = new ArrayList<>((int)((limit + (chunkSize / 2)) / chunkSize));

        try {
            long allocated = 0;
            int idx = 0;

            while (allocated < limit) {
                long size = Math.min(chunkSize, limit - allocated);

                File file = new File(allocationPath, ALLOCATOR_FILE_PREFIX + idx);

                MappedFile mappedFile = new MappedFile(file, size);

                mappedFiles.add(mappedFile);

                allocated += size;

                idx++;
            }
        }
        catch (IOException e) {
            // Close all files allocated so far.
            try {
                for (MappedFile mapped : mappedFiles)
                    mapped.close();
            }
            catch (IOException e0) {
                e.addSuppressed(e0);
            }

            throw new IgniteException(e);
        }

        log.info("Allocated clean memory state at location: " + allocationPath.getAbsolutePath());
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        for (MappedFile file : mappedFiles) {
            try {
                file.close();
            }
            catch (IOException e) {
                log.error("Failed to close memory-mapped file upon stop (will ignore) [file=" +
                    file.file() + ", err=" + e.getMessage() + ']');
            }
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public DirectMemory memory() {
        return new DirectMemory(restored, (List)mappedFiles);
    }
}
