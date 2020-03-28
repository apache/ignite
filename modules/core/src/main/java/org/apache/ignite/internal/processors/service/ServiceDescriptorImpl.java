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

package org.apache.ignite.internal.processors.service;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Service descriptor.
 *
 * @deprecated This implementation is based on {@code GridServiceDeployment} which has been deprecated because of
 * services internals use messages for deployment management instead of the utility cache, since Ignite 2.8.
 */
@Deprecated
public class ServiceDescriptorImpl implements ServiceDescriptor {
    /** */
    private static final long serialVersionUID = 0L;

    /** Configuration. */
    @GridToStringInclude
    private final GridServiceDeployment dep;

    /** Topology snapshot. */
    @GridToStringInclude
    private Map<UUID, Integer> top;

    /**
     * @param dep Deployment.
     */
    public ServiceDescriptorImpl(GridServiceDeployment dep) {
        this.dep = dep;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return dep.configuration().getName();
    }

    /** {@inheritDoc} */
    @Override public Class<? extends Service> serviceClass() {
        ServiceConfiguration cfg = dep.configuration();

        if (cfg instanceof LazyServiceConfiguration) {
            String clsName = ((LazyServiceConfiguration)cfg).serviceClassName();

            try {
                return (Class<? extends Service>)Class.forName(clsName);
            }
            catch (ClassNotFoundException e) {
                throw new IgniteException("Failed to find service class: " + clsName, e);
            }
        }
        else
            return dep.configuration().getService().getClass();
    }

    /** {@inheritDoc} */
    @Override public int totalCount() {
        return dep.configuration().getTotalCount();
    }

    /** {@inheritDoc} */
    @Override public int maxPerNodeCount() {
        return dep.configuration().getMaxPerNodeCount();
    }

    /** {@inheritDoc} */
    @Nullable @Override public String cacheName() {
        return dep.configuration().getCacheName();
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K> K affinityKey() {
        return (K)dep.configuration().getAffinityKey();
    }

    /** {@inheritDoc} */
    @Override public UUID originNodeId() {
        return dep.nodeId();
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, Integer> topologySnapshot() {
        return top;
    }

    /**
     * @param top Topology snapshot.
     */
    void topologySnapshot(Map<UUID, Integer> top) {
        this.top = top;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceDescriptorImpl.class, this);
    }
}
