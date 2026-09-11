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

package org.apache.ignite.compatibility.testframework.testcontainers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;

/** Ignite cluster container. */
public class IgniteClusterContainer implements Startable {
    /** Containers. */
    private final List<IgniteContainer> containers;

    /** Network. */
    protected final Network net = Network.newNetwork();

    /** Image name. */
    protected final String imageName;

    /** Consistent ID's. */
    protected final List<String> consistentIds;

    /** Whether the cluster has been started, guarding against a second {@link #start()}. */
    private boolean started;

    /**
     * @param imageName Image name.
     * @param consistentIds Consistent ID's.
     */
    public IgniteClusterContainer(String imageName, List<String> consistentIds) {
        this.imageName = imageName;
        this.consistentIds = consistentIds;

        containers = new ArrayList<>(consistentIds.size());
    }

    /**
     * Factory hook for the node container. Overrides only receive {@code idx}; the image name, network and
     * consistent IDs are instance fields (see {@link #imageName}, {@link #net}, {@link #consistentIds}).
     *
     * @param idx Node index.
     * @return The node container.
     */
    protected IgniteContainer container(int idx) throws Exception {
        return new IgniteContainer(imageName, net, "node" + (1 + idx), consistentIds.get(idx), idx);
    }

    /** Builds the node containers. */
    protected void initContainers() throws Exception {
        for (int i = 0; i < consistentIds.size(); i++)
            containers.add(container(i));
    }

    /** {@inheritDoc} */
    @Override public void start() {
        // Idempotent: either the cluster already started successfully, or container creation succeeded
        // but startup (deepStart/activateCluster) failed on a previous attempt — in both cases the
        // containers list is already populated and must not be built a second time (duplicate hostnames,
        // consistent IDs and fixed host ports would make the baseline unreachable).
        if (started || !containers.isEmpty())
            return;

        try {
            initContainers();
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }

        Startables.deepStart(containers).join();

        containers.get(0).activateCluster(containers.size());

        started = true;
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        for (IgniteContainer container : containers)
            container.stop();

        net.close();
    }

    /** */
    public List<IgniteContainer> containers() {
        return Collections.unmodifiableList(containers);
    }
}
