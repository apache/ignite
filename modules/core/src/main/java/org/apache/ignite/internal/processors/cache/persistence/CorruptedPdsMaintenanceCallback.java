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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceWorkflowCallback;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DATA_FILENAME;

/**
 *
 */
public class CorruptedPdsMaintenanceCallback implements MaintenanceWorkflowCallback {
    /** */
    private final File workDir;

    /** */
    private final List<String> cacheStoreDirs;

    /**
     * @param workDir
     * @param cacheStoreDirs
     */
    public CorruptedPdsMaintenanceCallback(
        @NotNull File workDir,
        @NotNull List<String> cacheStoreDirs
    ) {
        this.workDir = workDir;
        this.cacheStoreDirs = cacheStoreDirs;
    }

    /** {@inheritDoc} */
    @Override public boolean shouldProceedWithMaintenance() {
        for (String cacheStoreDirName : cacheStoreDirs) {
            File cacheStoreDir = new File(workDir, cacheStoreDirName);

            if (cacheStoreDir.exists()
                && cacheStoreDir.isDirectory()
                && cacheStoreDir.listFiles().length > 0
            ) {
                for (File f : cacheStoreDir.listFiles()) {
                    if (!f.getName().equals(CACHE_DATA_FILENAME))
                        return true;
                }
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public List<MaintenanceAction<?>> allActions() {
        return Arrays.asList(
            new CleanCacheStoresMaintenanceAction(workDir, cacheStoreDirs.toArray(new String[0])),
            new CheckCorruptedCacheStoresCleanAction(workDir, cacheStoreDirs.toArray(new String[0])));
    }

    /** {@inheritDoc} */
    @Override public MaintenanceAction<?> automaticAction() {
        return null;
    }
}
