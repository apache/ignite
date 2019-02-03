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
package org.apache.ignite.internal.commandline.cache.reset_lost_partitions;

import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Reset status of lost partitions.
 */
public class CacheResetLostPartitionsTask extends VisorOneNodeTask<CacheResetLostPartitionsTaskArg, CacheResetLostPartitionsTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<CacheResetLostPartitionsTaskArg, CacheResetLostPartitionsTaskResult> job(
        CacheResetLostPartitionsTaskArg arg) {
        return new CacheResetLostPartitionsJob(arg, debug);
    }

    /** Job for node. */
    private static class CacheResetLostPartitionsJob extends VisorJob<CacheResetLostPartitionsTaskArg, CacheResetLostPartitionsTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Argument.
         * @param debug Debug.
         */
        public CacheResetLostPartitionsJob(@Nullable CacheResetLostPartitionsTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override public CacheResetLostPartitionsTaskResult run(
            CacheResetLostPartitionsTaskArg arg) throws IgniteException {
            try {
                final CacheResetLostPartitionsTaskResult res = new CacheResetLostPartitionsTaskResult();
                res.setMessageMap(new HashMap<>());

                if (!F.isEmpty(arg.getCaches())) {
                    for (String groupName : arg.getCaches()) {
                        final int grpId = CU.cacheId(groupName);

                        CacheGroupContext grp = ignite.context().cache().cacheGroup(grpId);

                        if (grp != null) {
                            SortedSet<String> cacheNames = grp.caches().stream()
                                .map(GridCacheContext::name)
                                .collect(Collectors.toCollection(TreeSet::new));

                            if (!F.isEmpty(cacheNames)) {
                                ignite.resetLostPartitions(cacheNames);

                                res.put(groupName, String.format("Reset LOST-partitions performed successfully. " +
                                        "Cache group (name = '%s', id = %d), caches (%s).",
                                    groupName, grpId, cacheNames));
                            }
                        }
                        else
                            res.put(groupName, String.format("Cache group (name = '%s', id = %d) not found.",
                                groupName, grpId));
                    }
                }

                return res;
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }

        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheResetLostPartitionsJob.class, this);
        }
    }
}
