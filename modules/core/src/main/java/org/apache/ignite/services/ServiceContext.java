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

package org.apache.ignite.services;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.resources.ServiceContextResource;
import org.jetbrains.annotations.Nullable;

/**
 * Service execution context. This context is provided using {@link ServiceContextResource} annotation and contains
 * information about specific service execution.
 *
 * @see ServiceContextResource
 */
public interface ServiceContext extends Serializable {
    /**
     * Gets service name.
     *
     * @return Service name.
     */
    public String name();

    /**
     * Gets service execution ID. Execution ID is guaranteed to be unique across
     * all service deployments.
     *
     * @return Service execution ID.
     */
    public UUID executionId();

    /**
     * Get flag indicating whether service has been cancelled or not.
     *
     * @return Flag indicating whether service has been cancelled or not.
     */
    public boolean isCancelled();

    /**
     * Gets cache name used for key-to-node affinity calculation. This parameter is optional
     * and is set only when key-affinity service was deployed.
     *
     * @return Cache name, possibly {@code null}.
     */
    @Nullable public String cacheName();

    /**
     * Gets affinity key used for key-to-node affinity calculation. This parameter is optional
     * and is set only when key-affinity service was deployed.
     *
     * @param <K> Affinity key type.
     * @return Affinity key, possibly {@code null}.
     */
    @Nullable public <K> K affinityKey();

    /**
     * Gets context of the current service call.
     *
     * @return Context of the current service call, possibly {@code null}.
     * @see ServiceCallContext
     */
    @Nullable public ServiceCallContext currentCallContext();
}
