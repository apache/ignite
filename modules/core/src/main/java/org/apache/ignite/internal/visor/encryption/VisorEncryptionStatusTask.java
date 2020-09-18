/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.encryption;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.managers.encryption.ReencryptStateUtils;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * The task for getting encryption status of the cache group.
 */
public class VisorEncryptionStatusTask extends VisorMultiNodeTask<String, Map<UUID, T2<Long, Long>>, T2<Long, Long>> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<String, T2<Long, Long>> job(String arg) {
        return new VisorGetCacheGroupKeysJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<UUID, T2<Long, Long>> reduce0(List<ComputeJobResult> results) {
        Map<UUID, T2<Long, Long>> resMap = new HashMap<>();

        for (ComputeJobResult res : results)
            resMap.put(res.getNode().id(), res.getData());

        return resMap;
    }

    /** The job for getting the master key name. */
    private static class VisorGetCacheGroupKeysJob extends VisorJob<String, T2<Long, Long>> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorGetCacheGroupKeysJob(String arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected T2<Long, Long> run(String arg) throws IgniteException {
            IgniteInternalCache<Object, Object> cache = ignite.context().cache().cache(arg);

            if (cache == null)
                throw new IgniteException("Cache " + arg + " not found.");

            CacheGroupContext grp = cache.context().group();
            GridEncryptionManager encrMgr = grp.shared().kernalContext().encryption();

            if (!encrMgr.reencryptionRequired(grp.groupId()))
                return new T2<>(0L, 0L);

            FilePageStoreManager mgr = (FilePageStoreManager)grp.shared().pageStore();

            long completePages = 0;
            long totalPages = 0;

            try {
                for (int p = 0; p < grp.affinity().partitions(); p++) {
                    PageStore pageStore = mgr.getStore(grp.groupId(), p);

                    if (!pageStore.exists())
                        continue;

                    long state = encrMgr.getEncryptionState(grp.groupId(), p);

                    totalPages += ReencryptStateUtils.pageCount(state);
                    completePages += ReencryptStateUtils.pageIndex(state);
                }

                long state = encrMgr.getEncryptionState(grp.groupId(), PageIdAllocator.INDEX_PARTITION);

                totalPages += ReencryptStateUtils.pageCount(state);
                completePages += ReencryptStateUtils.pageIndex(state);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            return new T2<>(completePages, totalPages);
        }
    }
}
