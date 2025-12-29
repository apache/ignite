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
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.management.wal.WalDisableCommand.WalDisableCommandArg;
import org.apache.ignite.internal.management.wal.WalEnableCommand.WalEnableCommandArg;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/** */
public class WalSetStateTask extends VisorMultiNodeTask<WalDisableCommandArg, WalSetStateTaskResult, WalSetStateTaskResult> {
    /** */
    private static final long serialVersionUID = 0;

    /** {@inheritDoc} */
    @Override protected VisorJob<WalDisableCommandArg, WalSetStateTaskResult> job(WalDisableCommandArg arg) {
        return new WalDisableJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable WalSetStateTaskResult reduce0(List<ComputeJobResult> res) throws IgniteException {
        Set<String> successGrps = new HashSet<>();
        List<String> errors = new ArrayList<>();

        for (ComputeJobResult jobRes : res) {
            if (jobRes.getException() != null) {
                Throwable e = jobRes.getException();
                errors.add("Node " + jobRes.getNode().consistentId() + ": Task execution failed - " + e.getMessage());
            }
            else {
                WalSetStateTaskResult result = jobRes.getData();
                if (result.successGroups() != null)
                    successGrps.addAll(result.successGroups());
                if (!Boolean.TRUE.equals(result.success()) && result.errorMessages() != null)
                    errors.addAll(result.errorMessages());
            }
        }

        if (errors.isEmpty())
            return new WalSetStateTaskResult(new ArrayList<>(successGrps));
        else
            return new WalSetStateTaskResult(new ArrayList<>(successGrps), errors);
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
                Set<String> availableGrps = new HashSet<>();

                for (CacheGroupContext gctx : ignite.context().cache().cacheGroups()) {
                    String grpName = gctx.cacheOrGroupName();
                    availableGrps.add(grpName);

                    if (requestedGrps != null && !requestedGrps.contains(grpName))
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

                for (String requestedGrp : requestedGrps) {
                    if (!availableGrps.contains(requestedGrp))
                        errors.add("Cache group not found: " + requestedGrp);
                }

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
