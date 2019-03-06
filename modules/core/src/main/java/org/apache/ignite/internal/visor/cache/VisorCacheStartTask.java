/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.visor.cache;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.util.VisorTaskUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Task that start cache or near cache with specified configuration.
 */
@GridInternal
@GridVisorManagementTask
public class VisorCacheStartTask extends VisorMultiNodeTask<VisorCacheStartTaskArg, Map<UUID, IgniteException>, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheStartJob job(VisorCacheStartTaskArg arg) {
        return new VisorCacheStartJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<UUID, IgniteException> reduce0(List<ComputeJobResult> results) throws IgniteException {
        Map<UUID, IgniteException> map = new HashMap<>();

        for (ComputeJobResult res : results)
            if (res.getException() != null)
                map.put(res.getNode().id(), res.getException());

        return map;
    }

    /**
     * Job that start cache or near cache with specified configuration.
     */
    private static class VisorCacheStartJob extends VisorJob<VisorCacheStartTaskArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg Contains cache name and XML configurations of cache.
         * @param debug Debug flag.
         */
        private VisorCacheStartJob(VisorCacheStartTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(VisorCacheStartTaskArg arg) throws IgniteException {
            String cfg = arg.getConfiguration();

            assert !F.isEmpty(cfg);

            try (ByteArrayInputStream bais = new ByteArrayInputStream(cfg.getBytes())) {
                if (arg.isNear()) {
                    NearCacheConfiguration nearCfg = Ignition.loadSpringBean(bais, "nearCacheConfiguration");

                    ignite.getOrCreateNearCache(VisorTaskUtils.unescapeName(arg.getName()), nearCfg);
                }
                else {
                    CacheConfiguration cacheCfg = Ignition.loadSpringBean(bais, "cacheConfiguration");

                    ignite.getOrCreateCache(cacheCfg);
                }
            }
            catch (IOException e) {
                throw new  IgniteException(e);
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheStartJob.class, this);
        }
    }
}
