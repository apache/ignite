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

package org.apache.ignite.internal.visor.persistence;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.persistence.CheckCorruptedCacheStoresCleanAction;
import org.apache.ignite.internal.processors.cache.persistence.CleanCacheStoresMaintenanceAction;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CORRUPTED_DATA_FILES_MNTC_TASK_ID;

/** */
@GridInternal
@GridVisorManagementTask
public class PersistenceTask extends VisorOneNodeTask<PersistenceTaskArg, PersistenceTaskResult> {
    @Override protected VisorJob<PersistenceTaskArg, PersistenceTaskResult> job(PersistenceTaskArg arg) {
        return new PersistenceJob(arg, debug);
    }

    /** */
    private static class PersistenceJob extends VisorJob<PersistenceTaskArg, PersistenceTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

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

            switch (arg.operation()) {
                case CLEAN:
                    return clean(arg);

                default:
                    return info();
            }
        }

        /** */
        private PersistenceTaskResult clean(PersistenceTaskArg arg) {
            PersistenceTaskResult res = new PersistenceTaskResult();

            PersistenceCleanSettings cleanSettings = arg.cleanSettings();

            GridCacheProcessor cacheProc = ignite.context().cache();
            MaintenanceRegistry mntcReg = ignite.context().maintenanceRegistry();

            switch (cleanSettings.cleanType()) {
                case ALL:
                    return cleanAll(cacheProc, mntcReg);

                case CORRUPTED:
                    return cleanCorrupted(mntcReg);

                case CACHES:
                    return cleanCaches(cacheProc, mntcReg, cleanSettings.cacheNames());
            }

            return res;
        }

        /** */
        private PersistenceTaskResult cleanCaches(
            GridCacheProcessor cacheProc,
            MaintenanceRegistry mntcReg,
            List<String> cacheNames
        ) {
            PersistenceTaskResult res = new PersistenceTaskResult(true);
            List<String> cleanedCaches = new ArrayList<>();
            List<String> failedToCleanCaches = new ArrayList<>();

            DataStorageConfiguration dsCfg = ignite.context().config().getDataStorageConfiguration();
            IgnitePageStoreManager pageStore = cacheProc.context().pageStore();

            for (String name : cacheNames) {
                DynamicCacheDescriptor cacheDescr = cacheProc.cacheDescriptor(name);

                if (CU.isPersistentCache(cacheDescr.cacheConfiguration(), dsCfg)) {
                    try {
                        pageStore.cleanupPersistentSpace(cacheDescr.cacheConfiguration());

                        cleanedCaches.add(FilePageStoreManager.cacheDirName(cacheDescr.cacheConfiguration()));
                    }
                    catch (IgniteCheckedException e) {
                        failedToCleanCaches.add(name);
                    }
                }
            }

            res.cleanedCaches(cleanedCaches);

            if (!failedToCleanCaches.isEmpty())
                res.failedToCleanCaches(failedToCleanCaches);

            List<MaintenanceAction> actions = mntcReg.actionsForMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_ID);

            Optional<MaintenanceAction> checkActionOpt = actions.stream().filter(a -> a.name().equals(CheckCorruptedCacheStoresCleanAction.ACTION_NAME))
                .findFirst();

            if (checkActionOpt.isPresent()) {
                MaintenanceAction<Boolean> action = checkActionOpt.get();

                res.maintenanceTaskCompleted(action.execute());
            }

            return res;
        }

        /** */
        private PersistenceTaskResult cleanAll(GridCacheProcessor cacheProc, MaintenanceRegistry mntcReg) {
            PersistenceTaskResult res = new PersistenceTaskResult(true);

            List<String> allCaches = cacheProc.cacheDescriptors()
                .values()
                .stream()
                .map(desc -> FilePageStoreManager.cacheDirName(desc.cacheConfiguration()))
                .collect(Collectors.toList());

            try {
                cacheProc.cleanupCachesDirectories();
            } catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }

            mntcReg.unregisterMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_ID);

            res.maintenanceTaskCompleted(true);
            res.cleanedCaches(allCaches);

            return res;
        }

        /** */
        private PersistenceTaskResult cleanCorrupted(MaintenanceRegistry mntcReg) {
            PersistenceTaskResult res = new PersistenceTaskResult(true);

            MaintenanceTask task = mntcReg.activeMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_ID);

            List<MaintenanceAction> actions = mntcReg
                .actionsForMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_ID);

            Optional<MaintenanceAction> cleanCorruptedActionOpt = actions
                .stream()
                .filter(a -> a.name().equals(CleanCacheStoresMaintenanceAction.ACTION_NAME))
                .findFirst();

            if (cleanCorruptedActionOpt.isPresent()) {
                cleanCorruptedActionOpt.get().execute();

                mntcReg.unregisterMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_ID);

                String params = task.parameters();

                if (params != null)
                    res.cleanedCaches(Arrays.asList(params.split(File.separator)));

                res.maintenanceTaskCompleted(true);
            }

            return res;
        }

        private PersistenceTaskResult info() {
            return null;
        }
    }
}
