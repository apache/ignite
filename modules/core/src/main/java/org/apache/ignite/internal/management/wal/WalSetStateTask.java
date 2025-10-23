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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.management.wal.WalDisableCommand.WalDisableCommandArg;
import org.apache.ignite.internal.management.wal.WalEnableCommand.WalEnableCommandArg;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/** */
public class WalSetStateTask extends VisorMultiNodeTask<WalDisableCommandArg, Void, Void> {
    /** */
    private static final long serialVersionUID = 0;

    /** {@inheritDoc} */
    @Override protected VisorJob<WalDisableCommandArg, Void> job(WalDisableCommandArg arg) {
        return new WalDisableJob(arg, false);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable Void reduce0(List<ComputeJobResult> res) throws IgniteException {
        return null;
    }

    /** */
    private static class WalDisableJob extends VisorJob<WalDisableCommandArg, Void> {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        protected WalDisableJob(@Nullable WalDisableCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(@Nullable WalDisableCommandArg arg) throws IgniteException {
            Set<String> grps = F.isEmpty(arg.groups()) ? null : new HashSet<>(Arrays.asList(arg.groups()));

            for (CacheGroupContext gctx : ignite.context().cache().cacheGroups()) {
                String grpName = gctx.cacheOrGroupName();

                if (grps != null && !grps.contains(grpName))
                    continue;

                GridCacheContext<?, ?> cctx = F.first(gctx.caches());

                if (cctx == null)
                    continue;

                if (arg instanceof WalEnableCommandArg)
                    ignite.cluster().enableWal(cctx.name());
                else
                    ignite.cluster().disableWal(cctx.name());
            }

            return null;
        }
    }
}
