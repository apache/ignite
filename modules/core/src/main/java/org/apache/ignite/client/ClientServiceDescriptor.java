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

package org.apache.ignite.client;

import java.util.UUID;
import org.apache.ignite.platform.PlatformType;
import org.apache.ignite.services.Service;
import org.jetbrains.annotations.Nullable;

/**
 * Descriptor of {@link Service}.
 */
public interface ClientServiceDescriptor {
    /**
     * Gets service name.
     *
     * @return Service name.
     */
    public String name();

    /**
     * Gets service class.
     *
     * @return Service class.
     */
    public String serviceClass();

    /**
     * Gets maximum allowed total number of deployed services in the grid, {@code 0} for unlimited.
     *
     * @return Maximum allowed total number of deployed services in the grid, {@code 0} for unlimited.
     */
    public int totalCount();

    /**
     * Gets maximum allowed number of deployed services on each node, {@code 0} for unlimited.
     *
     * @return Maximum allowed total number of deployed services on each node, {@code 0} for unlimited.
     */
    public int maxPerNodeCount();

    /**
     * Gets cache name used for key-to-node affinity calculation. This parameter is optional
     * and is set only when key-affinity service was deployed.
     *
     * @return Cache name, possibly {@code null}.
     */
    @Nullable public String cacheName();

    /**
     * Gets ID of grid node that initiated the service deployment.
     *
     * @return ID of grid node that initiated the service deployment.
     */
    public UUID originNodeId();

    /**
     * Gets platform type.
     *
     * @return Platform type.
     */
    public PlatformType platformType();
}
