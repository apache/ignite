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

package org.apache.ignite.internal.management.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.management.persistence.PersistenceCommand.PersistenceBackupAllTaskArg;
import org.apache.ignite.internal.management.persistence.PersistenceCommand.PersistenceBackupCorruptedTaskArg;
import org.apache.ignite.internal.management.persistence.PersistenceCommand.PersistenceCleanAllTaskArg;
import org.apache.ignite.internal.management.persistence.PersistenceCommand.PersistenceCleanCorruptedTaskArg;
import org.apache.ignite.internal.management.persistence.PersistenceCommand.PersistenceTaskArg;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.persistence.CheckCorruptedCacheStoresCleanAction;
import org.apache.ignite.internal.processors.cache.persistence.CleanCacheStoresMaintenanceAction;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CORRUPTED_DATA_FILES_MNTC_TASK_NAME;

/** */
@GridInternal
public class PersistenceTask extends VisorOneNodeTask<PersistenceTaskArg, PersistenceTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final String BACKUP_FOLDER_PREFIX = "backup_";

    /** {@inheritDoc} */
    @Override protected VisorJob<PersistenceTaskArg, PersistenceTaskResult> job(PersistenceTaskArg arg) {
        return new PersistenceJob(arg, debug);
    }

    /** */
    private static class PersistenceJob extends VisorJob<PersistenceTaskArg, PersistenceTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Node file tree. */
        private transient NodeFileTree ft;

        /**
         * Create job with specified argument.
         *
         * @param arg   Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected PersistenceJob(@Nullable PersistenceTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected PersistenceTaskResult run(@Nullable PersistenceTaskArg arg) throws IgniteException {
            if (!ignite.context().maintenanceRegistry().isMaintenanceMode())
                return new PersistenceTaskResult(false);

            ft = ignite.context().pdsFolderResolver().fileTree();

            if (arg instanceof PersistenceCleanAllTaskArg
                || arg instanceof PersistenceCleanCorruptedTaskArg
                || arg instanceof PersistenceCleanCachesTaskArg)
                return clean(arg);
            else if (arg instanceof PersistenceBackupAllTaskArg
                || arg instanceof PersistenceBackupCorruptedTaskArg
                || arg instanceof PersistenceBackupCachesTaskArg)
                return backup(arg);
            else
                return info();
        }

        /** */
        private PersistenceTaskResult backup(PersistenceTaskArg arg) {
            MaintenanceRegistry mntcReg = ignite.context().maintenanceRegistry();
            MaintenanceTask task = mntcReg.activeMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME);

            if (arg instanceof PersistenceBackupAllTaskArg)
                return backupAll();
            else if (arg instanceof PersistenceBackupCorruptedTaskArg)
                return backupCaches(corruptedCacheDirectories(task));
            else
                return backupCaches(cacheDirectoriesFromCacheNames(((PersistenceBackupCachesTaskArg)arg).caches()));
        }

        /** */
        private PersistenceTaskResult backupAll() {
            GridCacheProcessor cacheProc = ignite.context().cache();

            List<String> allCacheDirs = cacheProc.cacheDescriptors()
                .values()
                .stream()
                .map(desc -> ft.cacheDirName(desc.cacheConfiguration()))
                .distinct()
                .collect(Collectors.toList());

            return backupCaches(allCacheDirs);
        }

        /** */
        private PersistenceTaskResult backupCaches(List<String> cacheDirs) {
            PersistenceTaskResult res = new PersistenceTaskResult(true);

            List<String> backupCompletedCaches = new ArrayList<>();
            List<String> backupFailedCaches = new ArrayList<>();

            for (String dir : cacheDirs) {
                String backupDirName = BACKUP_FOLDER_PREFIX + dir;

                File backupDir = new File(ft.nodeStorage(), backupDirName);

                if (!backupDir.exists()) {
                    try {
                        U.ensureDirectory(backupDir, backupDirName, null);

                        copyCacheFiles(ft.nodeStorage().toPath().resolve(dir).toFile(), backupDir);

                        backupCompletedCaches.add(backupDirName);
                    }
                    catch (IgniteCheckedException | IOException e) {
                        backupFailedCaches.add(dir);
                    }
                }
            }

            res.handledCaches(backupCompletedCaches);
            res.failedCaches(backupFailedCaches);

            return res;
        }

        /** */
        private void copyCacheFiles(File sourceDir, File backupDir) throws IOException {
            for (File f : sourceDir.listFiles())
                Files.copy(f.toPath(), backupDir.toPath().resolve(f.getName()), StandardCopyOption.REPLACE_EXISTING);
        }

        /** */
        private PersistenceTaskResult clean(PersistenceTaskArg arg) {
            PersistenceTaskResult res = new PersistenceTaskResult();

            GridCacheProcessor cacheProc = ignite.context().cache();
            MaintenanceRegistry mntcReg = ignite.context().maintenanceRegistry();

            if (arg instanceof PersistenceCleanAllTaskArg)
                return cleanAll(cacheProc, mntcReg);
            else if (arg instanceof PersistenceCleanCorruptedTaskArg)
                return cleanCorrupted(mntcReg);
            else if (arg instanceof PersistenceCleanCachesTaskArg)
                return cleanCaches(cacheProc, mntcReg, ((PersistenceCleanCachesTaskArg)arg).caches());

            return res;
        }

        /** */
        private PersistenceTaskResult cleanCaches(
            GridCacheProcessor cacheProc,
            MaintenanceRegistry mntcReg,
            String[] cacheNames
        ) {
            PersistenceTaskResult res = new PersistenceTaskResult(true);

            List<String> cleanedCaches = new ArrayList<>();
            List<String> failedToCleanCaches = new ArrayList<>();

            DataStorageConfiguration dsCfg = ignite.context().config().getDataStorageConfiguration();
            IgnitePageStoreManager pageStore = cacheProc.context().pageStore();

            AtomicReference<String> missedCache = new AtomicReference<>();

            Boolean allExist = Arrays.stream(cacheNames)
                .map(name -> {
                    if (cacheProc.cacheDescriptor(name) != null)
                        return true;
                    else {
                        missedCache.set(name);

                        return false;
                    }
                })
                .reduce(true, (t, u) -> t && u);

            if (!allExist)
                throw new IllegalArgumentException("Cache with name " + missedCache.get() +
                    " not found, no caches will be cleaned.");

            for (String name : cacheNames) {
                DynamicCacheDescriptor cacheDescr = cacheProc.cacheDescriptor(name);

                if (CU.isPersistentCache(cacheDescr.cacheConfiguration(), dsCfg)) {
                    try {
                        pageStore.cleanupPersistentSpace(cacheDescr.cacheConfiguration());

                        cleanedCaches.add(ft.cacheDirName(cacheDescr.cacheConfiguration()));
                    }
                    catch (IgniteCheckedException e) {
                        failedToCleanCaches.add(name);
                    }
                }
            }

            res.handledCaches(cleanedCaches);

            if (!failedToCleanCaches.isEmpty())
                res.failedCaches(failedToCleanCaches);

            List<MaintenanceAction<?>> actions = mntcReg.actionsForMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME);

            Optional<MaintenanceAction<?>> checkActionOpt = actions.stream()
                .filter(a -> a.name().equals(CheckCorruptedCacheStoresCleanAction.ACTION_NAME))
                .findFirst();

            if (checkActionOpt.isPresent()) {
                MaintenanceAction<Boolean> action = (MaintenanceAction<Boolean>)checkActionOpt.get();

                Boolean mntcTaskCompleted = action.execute();

                res.maintenanceTaskCompleted(mntcTaskCompleted);

                if (mntcTaskCompleted)
                    mntcReg.unregisterMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME);
            }

            return res;
        }

        /** */
        private PersistenceTaskResult cleanAll(GridCacheProcessor cacheProc, MaintenanceRegistry mntcReg) {
            PersistenceTaskResult res = new PersistenceTaskResult(true);

            List<String> allCacheDirs = cacheProc.cacheDescriptors()
                .values()
                .stream()
                .map(desc -> ft.cacheDirName(desc.cacheConfiguration()))
                .collect(Collectors.toList());

            try {
                cacheProc.cleanupCachesDirectories();
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }

            mntcReg.unregisterMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME);

            res.maintenanceTaskCompleted(true);
            res.handledCaches(allCacheDirs);

            return res;
        }

        /** */
        private PersistenceTaskResult cleanCorrupted(MaintenanceRegistry mntcReg) {
            PersistenceTaskResult res = new PersistenceTaskResult(true);

            List<MaintenanceAction<?>> actions = mntcReg
                .actionsForMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME);

            Optional<MaintenanceAction<?>> cleanCorruptedActionOpt = actions
                .stream()
                .filter(a -> a.name().equals(CleanCacheStoresMaintenanceAction.ACTION_NAME))
                .findFirst();

            if (cleanCorruptedActionOpt.isPresent()) {
                cleanCorruptedActionOpt.get().execute();

                MaintenanceTask corruptedTask = mntcReg.activeMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME);

                mntcReg.unregisterMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME);

                res.handledCaches(
                    corruptedCacheDirectories(corruptedTask)
                );

                res.maintenanceTaskCompleted(true);
            }

            return res;
        }

        /** */
        private PersistenceTaskResult info() {
            PersistenceTaskResult res = new PersistenceTaskResult(true);

            GridCacheProcessor cacheProc = ignite.context().cache();
            DataStorageConfiguration dsCfg = ignite.context().config().getDataStorageConfiguration();

            MaintenanceTask task = ignite.context().maintenanceRegistry()
                .activeMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME);

            if (task == null)
                return res;

            List<String> corruptedCacheNames = corruptedCacheDirectories(task);

            Map<String, IgniteBiTuple<Boolean, Boolean>> cachesInfo = new HashMap<>();

            for (DynamicCacheDescriptor desc : cacheProc.cacheDescriptors().values()) {
                if (!CU.isPersistentCache(desc.cacheConfiguration(), dsCfg))
                    continue;

                CacheGroupDescriptor grpDesc = desc.groupDescriptor();

                if (grpDesc != null) {
                    boolean globalWalEnabled = grpDesc.walEnabled();
                    boolean locWalEnabled = true;

                    if (globalWalEnabled && corruptedCacheNames.contains(desc.cacheName()))
                        locWalEnabled = false;

                    cachesInfo.put(desc.cacheName(), new IgniteBiTuple<>(globalWalEnabled, locWalEnabled));
                }
            }

            res.cachesInfo(cachesInfo);

            return res;
        }

        /** */
        private List<String> corruptedCacheDirectories(MaintenanceTask task) {
            String params = task.parameters();

            String[] namesArr = params.split(Pattern.quote(File.separator));

            return Arrays.asList(namesArr);
        }

        /** */
        private List<String> cacheDirectoriesFromCacheNames(String[] cacheNames) {
            GridCacheProcessor cacheProc = ignite.context().cache();

            DataStorageConfiguration dsCfg = ignite.configuration().getDataStorageConfiguration();

            AtomicReference<String> missedCache = new AtomicReference<>();

            Boolean allExist = Arrays.stream(cacheNames)
                .map(s -> {
                    if (cacheProc.cacheDescriptor(s) != null)
                        return true;
                    else {
                        missedCache.set(s);

                        return false;
                    }
                })
                .reduce(true, (u, v) -> u && v);

            if (!allExist)
                throw new IllegalArgumentException("Cache with name " + missedCache.get() +
                    " not found, no caches will be backed up.");

            return Arrays.stream(cacheNames)
                .filter(s -> cacheProc.cacheDescriptor(s) != null)
                .filter(s ->
                    CU.isPersistentCache(cacheProc.cacheDescriptor(s).cacheConfiguration(), dsCfg))
                .map(s -> cacheProc.cacheDescriptor(s).cacheConfiguration())
                .map(ft::cacheDirName)
                .distinct()
                .collect(Collectors.toList());
        }
    }
}
