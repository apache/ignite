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

package org.apache.ignite.internal.management.wal;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isCdcEnabled;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isPersistenceEnabled;

/**
 * Get state of WAL on each server node.
 */
@GridInternal
public class WalStateTask extends
    VisorMultiNodeTask<WalStateCommandArg, List<WalStateTask.NodeWalState>, WalStateTask.NodeWalState> {
    /** */
    private static final long serialVersionUID = 0;

    /** {@inheritDoc} */
    @Override protected VisorJob<WalStateCommandArg, NodeWalState> job(WalStateCommandArg arg) {
        return new WalStateJob(arg, false);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable List<NodeWalState> reduce0(List<ComputeJobResult> res) throws IgniteException {
        return res.stream()
            .peek(r -> {
                if (r.getException() != null)
                    throw r.getException();
            })
            .map(r -> (NodeWalState)r.getData())
            .collect(Collectors.toList());
    }

    /** */
    private static class WalStateJob extends VisorJob<WalStateCommandArg, NodeWalState> {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        protected WalStateJob(@Nullable WalStateCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected NodeWalState run(@Nullable WalStateCommandArg arg) throws IgniteException {
            boolean walDisabled = !isPersistenceEnabled(ignite.configuration()) && !isCdcEnabled(ignite.configuration());

            Set<String> grps = F.isEmpty(arg.groups()) ? null : new HashSet<>(Arrays.asList(arg.groups()));

            Map<String, GroupWalState> res = new HashMap<>();

            for (CacheGroupContext gctx : ignite.context().cache().cacheGroups()) {
                String grpName = gctx.cacheOrGroupName();

                if (grps != null && !grps.contains(grpName))
                    continue;

                if (walDisabled || !gctx.persistenceEnabled())
                    res.put(grpName, new GroupWalState(false, false, false));
                else
                    res.put(grpName, new GroupWalState(gctx.globalWalEnabled(), gctx.localWalEnabled(), gctx.indexWalEnabled()));
            }

            return new NodeWalState(
                ignite.localNode().id(),
                ignite.localNode().consistentId(),
                walDisabled ? null : ignite.configuration().getDataStorageConfiguration().getWalMode(),
                res
            );
        }
    }

    /** */
    public static class NodeWalState implements Serializable {
        /** */
        private static final long serialVersionUID = 0;

        /** @see IgniteConfiguration#getNodeId() */
        final UUID id;

        /** @see IgniteConfiguration#getConsistentId() */
        final Object consId;

        /** @see DataStorageConfiguration#setWalMode(WALMode) */
        @Nullable final WALMode mode;

        /** */
        final Map<String, GroupWalState> states;

        /** */
        public NodeWalState(UUID id, Object consId, @Nullable WALMode mode, Map<String, GroupWalState> states) {
            this.id = id;
            this.consId = consId;
            this.mode = mode;
            this.states = states;
        }
    }

    /** Global, Local, Index states of WAL for group. */
    public static class GroupWalState extends GridTuple3<Boolean, Boolean, Boolean> {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        public GroupWalState() {
            // No-op.
        }

        /** */
        public GroupWalState(@Nullable Boolean val1, @Nullable Boolean val2, @Nullable Boolean val3) {
            super(val1, val2, val3);
        }

        /** */
        boolean globalWalEnabled() {
            return get1();
        }

        /** */
        boolean localWalEnabled() {
            return get2();
        }

        /** */
        boolean indexWalEnabled() {
            return get3();
        }
    }
}
