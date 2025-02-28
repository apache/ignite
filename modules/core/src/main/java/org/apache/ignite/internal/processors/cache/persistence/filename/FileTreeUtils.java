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
import java.io.IOException;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Utility methods for {@link NodeFileTree}
 */
public class FileTreeUtils {
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
}
