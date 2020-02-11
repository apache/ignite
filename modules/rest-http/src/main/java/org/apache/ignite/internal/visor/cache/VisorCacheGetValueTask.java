/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.cache;

import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.util.VisorTaskUtils;

/**
 * Task that get value in specified cache for specified key value.
 */
@GridInternal
@GridVisorManagementTask
public class VisorCacheGetValueTask extends VisorOneNodeTask<VisorCacheGetValueTaskArg, VisorCacheModifyTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheGetValueJob job(VisorCacheGetValueTaskArg arg) {
        return new VisorCacheGetValueJob(arg, debug);
    }

    /**
     * Job that get value in specified cache for specified key value.
     */
    private static class VisorCacheGetValueJob extends VisorJob<VisorCacheGetValueTaskArg, VisorCacheModifyTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg Task argument.
         * @param debug Debug flag.
         */
        private VisorCacheGetValueJob(VisorCacheGetValueTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorCacheModifyTaskResult run(final VisorCacheGetValueTaskArg arg) {
            assert arg != null;

            String cacheName = arg.getCacheName();
            assert cacheName != null;

            IgniteCache<Object, Object> cache = ignite.cache(cacheName);

            if (cache == null)
                throw new IllegalArgumentException("Failed to find cache with specified name: " + arg.getCacheName());

            Object key = arg.getKeyValueHolder().getKey();

            assert key != null;

            ClusterNode node = ignite.affinity(cacheName).mapKeyToNode(key);

            UUID nid = node != null ? node.id() : null;

            Object val = cache.withKeepBinary().get(key);

            return new VisorCacheModifyTaskResult(
                nid,
                val instanceof BinaryObject
                    ? U.compact(((BinaryObject)val).type().typeName())
                    : VisorTaskUtils.compactClass(val),
                val
            );
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheGetValueJob.class, this);
        }
    }
}
