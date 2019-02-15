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

import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.query.VisorQueryUtils;
import org.apache.ignite.internal.visor.util.VisorTaskUtils;

/**
 * Task that modify value in specified cache.
 */
@GridInternal
@GridVisorManagementTask
public class VisorCacheModifyTask extends VisorOneNodeTask<VisorCacheModifyTaskArg, VisorCacheModifyTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheModifyJob job(VisorCacheModifyTaskArg arg) {
        return new VisorCacheModifyJob(arg, debug);
    }

    /**
     * Job that modify value in specified cache.
     */
    private static class VisorCacheModifyJob extends VisorJob<VisorCacheModifyTaskArg, VisorCacheModifyTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg Task argument.
         * @param debug Debug flag.
         */
        private VisorCacheModifyJob(VisorCacheModifyTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorCacheModifyTaskResult run(final VisorCacheModifyTaskArg arg) {
            assert arg != null;

            VisorModifyCacheMode mode = arg.getMode();
            String cacheName = arg.getCacheName();
            Object key = arg.getKey();

            assert mode != null;
            assert cacheName != null;
            assert key != null;

            IgniteCache<Object, Object> cache = ignite.cache(cacheName);

            if (cache == null)
                throw new IllegalArgumentException("Failed to find cache with specified name [cacheName=" + arg.getCacheName() + "]");

            ClusterNode node = ignite.affinity(cacheName).mapKeyToNode(key);

            UUID nid = node != null ? node.id() : null;

            switch (mode) {
                case PUT:
                    Object old = cache.get(key);

                    cache.put(key, arg.getValue());

                    return new VisorCacheModifyTaskResult(nid, VisorTaskUtils.compactClass(old),
                        VisorQueryUtils.convertValue(old));

                case GET:
                    Object val = cache.get(key);

                    return new VisorCacheModifyTaskResult(nid, VisorTaskUtils.compactClass(val),
                        VisorQueryUtils.convertValue(val));

                case REMOVE:
                    Object rmv = cache.get(key);

                    cache.remove(key);

                    return new VisorCacheModifyTaskResult(nid, VisorTaskUtils.compactClass(rmv),
                        VisorQueryUtils.convertValue(rmv));
            }

            return new VisorCacheModifyTaskResult(nid, null, null);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheModifyJob.class, this);
        }
    }
}
