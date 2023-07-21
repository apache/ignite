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

package org.apache.ignite.internal.visor.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Task that collect cache names and deployment IDs.
 */
@GridInternal
public class VisorCacheNamesCollectorTask extends VisorOneNodeTask<Void, VisorCacheNamesCollectorTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheNamesCollectorJob job(Void arg) {
        return new VisorCacheNamesCollectorJob(arg, debug);
    }

    /**
     * Job that collect cache names and deployment IDs.
     */
    private static class VisorCacheNamesCollectorJob extends VisorJob<Void, VisorCacheNamesCollectorTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg Task argument.
         * @param debug Debug flag.
         */
        private VisorCacheNamesCollectorJob(Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorCacheNamesCollectorTaskResult run(Void arg) {
            GridCacheProcessor cacheProc = ignite.context().cache();

            Map<String, IgniteUuid> caches = new HashMap<>();
            Set<String> groups = new HashSet<>();

            for (Map.Entry<String, DynamicCacheDescriptor> item : cacheProc.cacheDescriptors().entrySet()) {
                DynamicCacheDescriptor cd = item.getValue();

                caches.put(item.getKey(), cd.deploymentId());

                String grp = cd.groupDescriptor().groupName();

                if (!F.isEmpty(grp))
                    groups.add(grp);
            }

            return new VisorCacheNamesCollectorTaskResult(caches, groups);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheNamesCollectorJob.class, this);
        }
    }
}
