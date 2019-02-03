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
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Memory provider implementation based on memory mapped file.
 * <p>
 * Doesn't support memory reuse semantics.
 */
public class MappedFileMemoryProvider implements DirectMemoryProvider {
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

    /** */
    private long[] sizes;

    /** */
    private List<MappedFile> mappedFiles;

    /** Flag shows if current memory provider have been already initialized. */
    private boolean isInit;

    /**
     * @param allocationPath Allocation path.
     */
    public MappedFileMemoryProvider(IgniteLogger log, File allocationPath) {
        this.log = log;
        this.allocationPath = allocationPath;
    }

    /** {@inheritDoc} */
    @Override public void initialize(long[] sizes) {
        if (isInit)
            throw new IgniteException("Second initialization does not allowed for current provider");

        this.sizes = sizes;

        mappedFiles = new ArrayList<>(sizes.length);

        if (!allocationPath.exists()) {
            if (!allocationPath.mkdirs())
                throw new IgniteException("Failed to initialize allocation path (make sure directory is " +
                    "writable for the current user): " + allocationPath);
        }

        if (!allocationPath.isDirectory())
            throw new IgniteException("Failed to initialize allocation path (path is a file): " + allocationPath);

        File[] files = allocationPath.listFiles(ALLOCATOR_FILTER);

        if (files.length != 0) {
            if (log.isInfoEnabled())
                log.info("Will clean up the following files upon start: " + Arrays.asList(files));

            for (File file : files) {
                if (!file.delete())
                    throw new IgniteException("Failed to delete allocated file on start (make sure file is not " +
                        "opened by another process and current user has enough rights): " + file);
            }
        }

        isInit = true;
    }

    /** {@inheritDoc} */
    @Override public void shutdown(boolean deallocate) {
        if (mappedFiles != null) {
            for (MappedFile file : mappedFiles) {
                try {
                    file.close();
                }
                catch (IOException e) {
                    log.error("Failed to close memory-mapped file upon stop (will ignore) [file=" +
                        file.file() + ", err=" + e.getMessage() + ']');
                }
            }

            mappedFiles = null;
        }
    }

    /** {@inheritDoc} */
    @Override public DirectMemoryRegion nextRegion() {
        try {
            if (mappedFiles.size() == sizes.length)
                return null;

            int idx = mappedFiles.size();

            long chunkSize = sizes[idx];

            File file = new File(allocationPath, ALLOCATOR_FILE_PREFIX + alignInt(idx));

            MappedFile mappedFile = new MappedFile(file, chunkSize);

            mappedFiles.add(mappedFile);

            return mappedFile;
        }
        catch (IOException e) {
            U.error(log, "Failed to allocate next memory-mapped region", e);

            return null;
        }
    }

    /**
     * @param idx Index.
     * @return 0-aligned string.
     */
    private static String alignInt(int idx) {
        String idxStr = String.valueOf(idx);

        StringBuilder res = new StringBuilder();

        for (int i = 0; i < 8 - idxStr.length(); i++)
            res.append('0');

        res.append(idxStr);

        return res.toString();
    }
}
