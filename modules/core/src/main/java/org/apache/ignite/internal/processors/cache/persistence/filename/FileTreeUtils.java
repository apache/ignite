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

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.TMP_WAL_SEG_FILE_EXT;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.TMP_ZIP_WAL_SEG_FILE_EXT;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.WAL_SEGMENT_FILE_EXT;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.ZIP_WAL_SEG_FILE_EXT;

/**
 * Utility methods for {@link NodeFileTree}
 */
public class FileTreeUtils {
    /** Pattern for segment file names. */
    private static final Pattern WAL_NAME_PATTERN = U.fixedLengthNumberNamePattern(WAL_SEGMENT_FILE_EXT);

    /** Pattern for WAL temp files - these files will be cleared at startup. */
    private static final Pattern WAL_TEMP_NAME_PATTERN = U.fixedLengthNumberNamePattern(TMP_WAL_SEG_FILE_EXT);

    /** */
    private static final Pattern WAL_SEGMENT_TEMP_FILE_COMPACTED_PATTERN = U.fixedLengthNumberNamePattern(TMP_ZIP_WAL_SEG_FILE_EXT);

    /** */
    public static final Pattern WAL_SEGMENT_FILE_COMPACTED_PATTERN = U.fixedLengthNumberNamePattern(ZIP_WAL_SEG_FILE_EXT);

    /** WAL segment file filter, see {@link #WAL_NAME_PATTERN} */
    public static final FileFilter WAL_SEGMENT_FILE_FILTER = file -> !file.isDirectory() &&
        WAL_NAME_PATTERN.matcher(file.getName()).matches();

    /** WAL segment temporary file filter, see {@link #WAL_TEMP_NAME_PATTERN} */
    public static final FileFilter WAL_SEGMENT_TEMP_FILE_FILTER = file -> !file.isDirectory() &&
        WAL_TEMP_NAME_PATTERN.matcher(file.getName()).matches();

    /** WAL segment file filter, see {@link #WAL_NAME_PATTERN} */
    public static final FileFilter WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER = file -> !file.isDirectory() &&
        (WAL_NAME_PATTERN.matcher(file.getName()).matches() ||
            WAL_SEGMENT_FILE_COMPACTED_PATTERN.matcher(file.getName()).matches());

    /** */
    public static final FileFilter WAL_SEGMENT_FILE_COMPACTED_FILTER = file -> !file.isDirectory() &&
        WAL_SEGMENT_FILE_COMPACTED_PATTERN.matcher(file.getName()).matches();

    /** */
    public static final FileFilter WAL_SEGMENT_TEMP_FILE_COMPACTED_FILTER = file -> !file.isDirectory() &&
        WAL_SEGMENT_TEMP_FILE_COMPACTED_PATTERN.matcher(file.getName()).matches();

    /** */
    private FileTreeUtils() {
        // No-op.
    }

    /**
     * Creates all cache storages for given tree.
     * @param ft Node file tree.
     * @param log Logger.
     */
    public static void createCacheStorages(NodeFileTree ft, IgniteLogger log) throws IgniteCheckedException {
        createAndCheck(ft.nodeStorage(), "page store work directory", log);

        for (Map.Entry<String, File> e : ft.dataRegionStorages().entrySet())
            createAndCheck(e.getValue(), "page store work directory[dataRegion=" + e.getKey() + ']', log);
    }

    /**
     * Removes temp snapshot files.
     *
     * @param sft Snapshot file tree.
     * @param err If {@code true} then operation ends with error.
     * @param log Logger.
     */
    public static void removeTmpSnapshotFiles(SnapshotFileTree sft, boolean err, IgniteLogger log) {
        NodeFileTree tmpFt = sft.tempFileTree();

        removeTmpDir(tmpFt.nodeStorage(), err, log);

        for (File tmpDrStorage : tmpFt.dataRegionStorages().values())
            removeTmpDir(tmpDrStorage, err, log);
    }

    /**
     * @param dir Directory to remove
     * @param err If {@code true} then operation ends with error.
     * @param log Logger.
     */
    private static void removeTmpDir(File dir, boolean err, IgniteLogger log) {
        U.delete(dir);

        // Delete snapshot directory if no other files exists.
        try {
            if (U.fileCount(dir.getParentFile().toPath()) == 0 || err)
                U.delete(dir.getParentFile().toPath());
        }
        catch (IOException e) {
            log.error("Snapshot directory doesn't exist [snpName=" + dir.getName() + ", dir=" + dir.getParentFile() + ']');
        }
    }

    /**
     * Creates given directory.
     * @param dir Directory to create.
     * @param desc Directory description for log
     * @param log Logger.
     * @throws IgniteCheckedException
     */
    private static void createAndCheck(File dir, String desc, IgniteLogger log) throws IgniteCheckedException {
        U.ensureDirectory(dir, desc, log);

        String tmpDir = System.getProperty("java.io.tmpdir");

        if (tmpDir != null && dir.getAbsolutePath().startsWith(tmpDir)) {
            log.warning("Persistence store directory is in the temp directory and may be cleaned." +
                "To avoid this set \"IGNITE_HOME\" environment variable properly or " +
                "change location of persistence directories in data storage configuration " +
                "(see DataStorageConfiguration#walPath, DataStorageConfiguration#walArchivePath, " +
                "DataStorageConfiguration#storagePath, DataRegionConfiguration#storagePath properties). " +
                "Current persistence store directory is: [" + dir.getAbsolutePath() + "]");
        }
    }

    /**
     * @param dir Directory.
     * @return {@code True} if directory conforms cache storage name pattern.
     */
    public static boolean cacheDir(File dir) {
        return dir.getName().startsWith(NodeFileTree.CACHE_DIR_PREFIX);
    }

    /**
     * @param f File.
     * @return {@code True} if file conforms partition file name pattern.
     */
    public static boolean partitionFile(File f) {
        return f.getName().startsWith(NodeFileTree.PART_FILE_PREFIX);
    }

    /**
     * @param f File.
     * @return {@code True} if file conforms cache config file name pattern.
     */
    public static boolean cacheConfigFile(File f) {
        return f.getName().equals(NodeFileTree.CACHE_DATA_FILENAME);
    }

    /**
     * @param f File.
     * @return {@code True} if file conforms cache config file name pattern.
     */
    public static boolean binFile(File f) {
        return f.getName().endsWith(NodeFileTree.FILE_SUFFIX);
    }

    /**
     * @param f File.
     * @return {@code True} if file conforms temp cache storage name pattern.
     */
    static boolean tmpCacheStorage(File f) {
        return f.isDirectory() && f.getName().startsWith(NodeFileTree.TMP_CACHE_DIR_PREFIX);
    }

    /**
     * @param f File.
     * @return {@code True} if file conforms temp cache configuration file name pattern.
     */
    public static boolean tmpCacheConfig(File f) {
        return f.getName().endsWith(NodeFileTree.CACHE_DATA_TMP_FILENAME);
    }

    /**
     * @param f File.
     * @return {@code True} if file is regular(not temporary).
     */
    public static boolean notTmpFile(File f) {
        return !f.getName().endsWith(NodeFileTree.TMP_SUFFIX);
    }

    /**
     * Check that file name matches segment name.
     *
     * @param name File name.
     * @return {@code True} if file name matches segment name.
     */
    public static boolean isSegmentFileName(@Nullable String name) {
        return name != null && (WAL_NAME_PATTERN.matcher(name).matches() ||
            WAL_SEGMENT_FILE_COMPACTED_PATTERN.matcher(name).matches());
    }
}
