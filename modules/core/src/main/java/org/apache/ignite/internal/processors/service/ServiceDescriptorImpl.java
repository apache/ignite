/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
 */
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
    @SuppressWarnings("unchecked")
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