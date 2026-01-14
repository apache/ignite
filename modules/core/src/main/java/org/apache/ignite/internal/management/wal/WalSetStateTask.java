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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.management.wal.WalDisableCommand.WalDisableCommandArg;
import org.apache.ignite.internal.management.wal.WalEnableCommand.WalEnableCommandArg;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/** */
public class WalSetStateTask extends VisorOneNodeTask<WalDisableCommandArg, WalSetStateTaskResult> {
    /** */
    private static final long serialVersionUID = 0;

    /** {@inheritDoc} */
    @Override protected VisorJob<WalDisableCommandArg, WalSetStateTaskResult> job(WalDisableCommandArg arg) {
        return new WalDisableJob(arg, debug);
    }

    /** */
    private static class WalDisableJob extends VisorJob<WalDisableCommandArg, WalSetStateTaskResult> {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        protected WalDisableJob(@Nullable WalDisableCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected WalSetStateTaskResult run(@Nullable WalDisableCommandArg arg) throws IgniteException {
            Set<String> requestedGrps = F.isEmpty(arg.groups()) ? null : new HashSet<>(Arrays.asList(arg.groups()));
            boolean isEnable = arg instanceof WalEnableCommandArg;
            List<String> successGrps = new ArrayList<>();
            List<String> errors = new ArrayList<>();

            try {
                for (CacheGroupContext gctx : ignite.context().cache().cacheGroups()) {
                    String grpName = gctx.cacheOrGroupName();

                    if (requestedGrps != null && !requestedGrps.remove(grpName))
                        continue;

                    try {
                        if (isEnable)
                            ignite.cluster().enableWal(grpName);
                        else
                            ignite.cluster().disableWal(grpName);

                        successGrps.add(grpName);
                    }
                    catch (Exception e) {
                        errors.add("Failed to " + (isEnable ? "enable" : "disable") +
                            " WAL for cache group: " + grpName + " - " + e.getMessage());
                    }
                }

                if (!requestedGrps.isEmpty())
                    errors.add("Cache groups not found: " + requestedGrps);

                if (errors.isEmpty())
                    return new WalSetStateTaskResult(successGrps);
                else
                    return new WalSetStateTaskResult(successGrps, errors);
            }
            catch (Exception e) {
                errors.add("Failed to execute operation - " + e.getMessage());

                return new WalSetStateTaskResult(successGrps, errors);
            }
        }
    }
}
