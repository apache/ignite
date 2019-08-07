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

import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Task that will find affinity node for key.
 */
@GridInternal
@GridVisorManagementTask
public class VisorCacheAffinityNodeTask extends VisorOneNodeTask<VisorCacheAffinityNodeTaskArg, UUID> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheAffinityNodeJob job(VisorCacheAffinityNodeTaskArg arg) {
        return new VisorCacheAffinityNodeJob(arg, debug);
    }

    /** Job that will find affinity node for key. */
    private static class VisorCacheAffinityNodeJob extends VisorJob<VisorCacheAffinityNodeTaskArg, UUID> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Cache name and key to find affinity node.
         * @param debug Debug flag.
         */
        private VisorCacheAffinityNodeJob(VisorCacheAffinityNodeTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected UUID run(@Nullable VisorCacheAffinityNodeTaskArg arg) throws IgniteException {
            assert arg != null;

            ClusterNode node = ignite.affinity(arg.getCacheName()).mapKeyToNode(arg.getKey());

            return node != null ? node.id() : null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheAffinityNodeJob.class, this);
        }
    }
}
