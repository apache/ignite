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

package org.apache.ignite.internal.processors.metric.list.view;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.metric.list.MonitoringRow;
import org.apache.ignite.internal.processors.service.ServiceInfo;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;

/** */
public class ServiceView implements MonitoringRow<IgniteUuid> {
    /** */
    private ServiceInfo serviceInfo;

    /**
     * @param serviceInfo Service info.
     * @param sessionId Session id.
     */
    public ServiceView(ServiceInfo serviceInfo) {
        this.serviceInfo = serviceInfo;
    }

    /** */
    public String name() {
        return serviceInfo.name();
    }

    /** */
    public IgniteUuid id() {
        return serviceInfo.serviceId();
    }

    /** */
    public Class<?> service() {
        return serviceInfo.serviceClass();
    }

    /** */
    public int totalCount() {
        return serviceInfo.configuration().getTotalCount();
    }

    /** */
    public int maxPerNodeCount() {
        return serviceInfo.configuration().getMaxPerNodeCount();
    }

    /** */
    public String cacheName() {
        return serviceInfo.configuration().getCacheName();
    }

    /** */
    public String affinityKey() {
        Object affKey = serviceInfo.configuration().getAffinityKey();

        return affKey == null ? null : affKey.toString();
    }

    /** */
    public Class<?> nodeFilter() {
        IgnitePredicate<ClusterNode> filter = serviceInfo.configuration().getNodeFilter();

        return filter == null ? null : filter.getClass();
    }

    /** */
    public boolean staticallyConfigured() {
        return serviceInfo.staticallyConfigured();
    }

    /** {@inheritDoc} */
    @Override public String sessionId() {
        return serviceInfo.originNodeId().toString();
    }
}
