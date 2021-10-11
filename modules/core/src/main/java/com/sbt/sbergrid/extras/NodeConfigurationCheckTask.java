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

package com.sbt.sbergrid.extras;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;

/**
 * Job collects {@link IgniteConfiguration} from all cluster nodes.
 */
@GridInternal
@GridVisorManagementTask
public class NodeConfigurationCheckTask implements ComputeTask<byte[], Map<UUID, List<String>>> {
    /** {@inheritDoc} */
    @Override public Map<UUID, List<String>> reduce(
        List<ComputeJobResult> results
    ) throws IgniteException {
        Map<UUID, List<String>> res = new HashMap<>();

        for (ComputeJobResult jobRes : results) {
            if (jobRes.getException() != null)
                throw new IgniteException(jobRes.getException());

            res.put(jobRes.getNode().id(), jobRes.getData());
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        byte[] arg
    ) throws IgniteException {
        Map<NodeConfigurationCollectorJob, ClusterNode> res = new HashMap<>();

        List<Function<IgniteConfiguration, String>> checks = new ArrayList<>();

        try (ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(arg))) {
            while (true)
                checks.add((Function<IgniteConfiguration, String>)((Class<?>)is.readObject()).newInstance());
        }
        catch (EOFException ignore) {
            // No-op
        }
        catch (IOException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new IgniteException(e);
        }

        for (ClusterNode node : subgrid)
            res.put(new NodeConfigurationCollectorJob(checks), node);

        return res;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(
        ComputeJobResult res,
        List<ComputeJobResult> rcvd
    ) throws IgniteException {
        return ComputeJobResultPolicy.WAIT;
    }

    /** Collects IgniteConfiguration from the single node. */
    private static class NodeConfigurationCollectorJob implements ComputeJob {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final List<Function<IgniteConfiguration, String>> checks;

        /** */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** @param checks Checks to check. */
        public NodeConfigurationCollectorJob(List<Function<IgniteConfiguration, String>> checks) {
            this.checks = checks;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            return checks.stream().map(check -> check.apply(ignite.configuration())).collect(Collectors.toList());
        }
    }

    /**
     * @param checks Serializes checks to byte array.
     * @return Byte array.
     */
    public static byte[] prepareChecks(List<Function<IgniteConfiguration, String>> checks) {
        try (ByteArrayOutputStream data = new ByteArrayOutputStream();
             ObjectOutputStream os = new ObjectOutputStream(data)) {
            for (Function<IgniteConfiguration, String> check : checks)
                os.writeObject(check.getClass());

            return data.toByteArray();
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }
}
