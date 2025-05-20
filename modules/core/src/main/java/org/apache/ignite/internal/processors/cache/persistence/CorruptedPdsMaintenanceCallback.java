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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceWorkflowCallback;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class CorruptedPdsMaintenanceCallback implements MaintenanceWorkflowCallback {
    /** */
    private final List<File> cacheStoreDirs;

    /**
     * @param ft Node file tree.
     * @param cacheStoreDirs Cache dirs names.
     */
    public CorruptedPdsMaintenanceCallback(
        @NotNull NodeFileTree ft,
        @NotNull List<String> cacheStoreDirs
    ) {
        this.cacheStoreDirs = new ArrayList<>();

        ft.allStorages().forEach(storageRoot -> {
            for (String cacheStoreDirName : cacheStoreDirs) {
                File cacheStoreDir = new File(storageRoot, cacheStoreDirName);

                if (cacheStoreDir.exists() && cacheStoreDir.isDirectory())
                    this.cacheStoreDirs.add(cacheStoreDir);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean shouldProceedWithMaintenance() {
        for (File cacheStoreDir : cacheStoreDirs) {
            File[] files = cacheStoreDir.listFiles();

            if (files == null)
                continue;

            for (File f : files) {
                if (!NodeFileTree.cacheConfigFile(f))
                    return true;
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public List<MaintenanceAction<?>> allActions() {
        return Arrays.asList(
            new CleanCacheStoresMaintenanceAction(cacheStoreDirs.toArray(new File[0])),
            new CheckCorruptedCacheStoresCleanAction(cacheStoreDirs.toArray(new File[0])));
    }

    /** {@inheritDoc} */
    @Override public MaintenanceAction<?> automaticAction() {
        return null;
    }
}
