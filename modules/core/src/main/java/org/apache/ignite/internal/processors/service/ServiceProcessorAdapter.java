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

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDescriptor;

/**
 * Adapter for different service processor implementations.
 */
public abstract class ServiceProcessorAdapter extends GridProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    protected ServiceProcessorAdapter(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param prj Grid projection.
     * @param name Service name.
     * @param srvc Service.
     * @return Future.
     */
    public abstract IgniteInternalFuture<?> deployNodeSingleton(ClusterGroup prj, String name, Service srvc);

    /**
     * @param name Service name.
     * @param srvc Service instance.
     * @return Future.
     */
    public abstract IgniteInternalFuture<?> deployClusterSingleton(ClusterGroup prj, String name, Service srvc);

    /**
     * @param name Service name.
     * @param srvc Service.
     * @param totalCnt Total count.
     * @param maxPerNodeCnt Max per-node count.
     * @return Future.
     */
    public abstract IgniteInternalFuture<?> deployMultiple(ClusterGroup prj, String name, Service srvc, int totalCnt,
        int maxPerNodeCnt);

    /**
     * @param name Service name.
     * @param srvc Service.
     * @param cacheName Cache name.
     * @param affKey Affinity key.
     * @return Future.
     */
    public abstract IgniteInternalFuture<?> deployKeyAffinitySingleton(String name, Service srvc, String cacheName,
        Object affKey);

    /**
     * @param prj Grid projection.
     * @param cfgs Service configurations.
     * @return Future for deployment.
     */
    public abstract IgniteInternalFuture<?> deployAll(ClusterGroup prj, Collection<ServiceConfiguration> cfgs);

    /**
     * @param name Service name.
     * @return Future.
     */
    public abstract IgniteInternalFuture<?> cancel(String name);

    /**
     * @return Future.
     */
    public abstract IgniteInternalFuture<?> cancelAll();

    /**
     * @param servicesNames Name of services to deploy.
     * @return Future.
     */
    public abstract IgniteInternalFuture<?> cancelAll(Collection<String> servicesNames);

    /**
     * @return Collection of service descriptors.
     */
    public abstract Collection<ServiceDescriptor> serviceDescriptors();

    /**
     * @param name Service name.
     * @param <T> Service type.
     * @return Service by specified service name.
     */
    public abstract <T> T service(String name);

    /**
     * @param prj Grid projection.
     * @param name Service name.
     * @param srvcCls Service class.
     * @param sticky Whether multi-node request should be done.
     * @param timeout If greater than 0 limits service acquire time. Cannot be negative.
     * @param <T> Service interface type.
     * @return The proxy of a service by its name and class.
     * @throws IgniteException If failed to create proxy.
     */
    public abstract <T> T serviceProxy(ClusterGroup prj, String name, Class<? super T> srvcCls, boolean sticky,
        long timeout) throws IgniteException;

    /**
     * @param name Service name.
     * @param <T> Service type.
     * @return Services by specified service name.
     */
    public abstract <T> Collection<T> services(String name);

    /**
     * @param name Service name.
     * @return Service by specified service name.
     */
    public abstract ServiceContextImpl serviceContext(String name);

    /**
     * @param name Service name.
     * @param timeout If greater than 0 limits task execution time. Cannot be negative.
     * @return Service topology.
     * @throws IgniteCheckedException On error.
     */
    public abstract Map<UUID, Integer> serviceTopology(String name, long timeout) throws IgniteCheckedException;

    /**
     * Callback for local join events for which the regular events are not generated.
     * <p/>
     * Local join event is expected in cases of joining to topology or client reconnect.
     *
     * @param evt Discovery event.
     * @param discoCache Discovery cache.
     */
    public void onLocalJoin(DiscoveryEvent evt, DiscoCache discoCache) {
        // No-op.
    }
}
