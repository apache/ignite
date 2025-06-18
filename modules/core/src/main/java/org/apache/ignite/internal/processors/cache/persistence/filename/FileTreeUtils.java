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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;

/**
 * Utility methods for {@link NodeFileTree}.
 */
public class FileTreeUtils {
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

        for (Map.Entry<String, File> e : ft.extraStorages().entrySet())
            createAndCheck(e.getValue(), "page store work directory [storagePath=" + e.getKey() + ']', log);
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

        removeTmpDir(tmpFt.root(), err, log);

        for (File tmpDrStorage : tmpFt.extraStorages().values())
            removeTmpDir(tmpDrStorage.getParentFile(), err, log);
    }

    /**
     * @param ccfg Cache configuration.
     * @param part Partition.
     * @return Storage path from config for partition.
     */
    public static @Nullable String partitionStorage(CacheConfiguration<?, ?> ccfg, int part) {
        if (part == INDEX_PARTITION && !F.isEmpty(ccfg.getIndexPath()))
            return ccfg.getIndexPath();

        String[] csp = ccfg.getStoragePaths();

        if (F.isEmpty(csp))
            return null;

        return resolveStorage(csp, part);
    }

    /**
     * @param dsCfg Data storage configuration.
     * @return All known storages.
     * @throws IgniteCheckedException
     */
    public static Set<String> nodeStorages(DataStorageConfiguration dsCfg) throws IgniteCheckedException {
        if (dsCfg == null)
            throw new IgniteCheckedException("Data storage must be configured");

        Set<String> nodeStorages = new HashSet<>(F.asList(dsCfg.getExtraStoragePaths()));

        if (!F.isEmpty(dsCfg.getStoragePath()))
            nodeStorages.add(dsCfg.getStoragePath());

        return nodeStorages;
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
            if (U.fileCount(dir.toPath()) == 0 || err)
                U.delete(dir.toPath());
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
     * @param storages Storages to select from.
     * @param part Partition.
     * @return Storage for partition.
     */
    static <T> T resolveStorage(T[] storages, int part) {
        return storages[part % storages.length];
    }
}
