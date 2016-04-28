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

import java.util.Arrays;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;

/**
 * Lazy service configuration.
 */
public class LazyServiceConfiguration extends ServiceConfiguration {
    /** */
    private static final long serialVersionUID = 0L;
    /** Service name. */
    private String name;

    /** Service instance. */
    @GridToStringExclude
    private transient volatile Service svc;

    /** */
    private final byte[] srvcBytes;

    /** Total count. */
    private int totalCnt;

    /** Max per-node count. */
    private int maxPerNodeCnt;

    /** Cache name. */
    private String cacheName;

    /** Affinity key. */
    private Object affKey;

    /** Node filter. */
    @GridToStringExclude
    private IgnitePredicate<ClusterNode> nodeFilter;

    /** */
    @SuppressWarnings("TransientFieldNotInitialized")
    private transient volatile GridKernalContext ctx;

    /**
     * @param cfg Configuration.
     */
    public LazyServiceConfiguration(ServiceConfiguration cfg, GridKernalContext ctx) {
        this.ctx = ctx;
        name = cfg.getName();
        totalCnt = cfg.getTotalCount();
        maxPerNodeCnt = cfg.getMaxPerNodeCount();
        cacheName = cfg.getCacheName();
        affKey = cfg.getAffinityKey();
        nodeFilter = cfg.getNodeFilter();

        Marshaller marsh = ctx.config().getMarshaller();

        svc = cfg.getService();

        try {
            srvcBytes = marsh.marshal(svc);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to marshal service with configured masrhaller [srvc=" + svc
                + ", marsh=" + marsh + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public Service getService() {
        // TODO double check?
        if (svc == null) {
            assert ctx != null: "Need to provide context before getting service.";

            Marshaller marshaller = ctx.config().getMarshaller();

            try {
                svc = marshaller.unmarshal(srvcBytes, ctx.config().getClassLoader());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to unmarshal service with configured masrhaller [marsh=" + marshaller
                    + ", srvcBytes=" + Arrays.toString(srvcBytes) + "]", e);
            }
        }

        U.dumpStack(">>>>> service: " + svc.getClass());

        return svc;
    }

    /** {@inheritDoc} */
    @Override public int getTotalCount() {
        return totalCnt;
    }

    /** {@inheritDoc} */
    @Override public int getMaxPerNodeCount() {
        return maxPerNodeCnt;
    }

    /** {@inheritDoc} */
    @Override public String getCacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public Object getAffinityKey() {
        return affKey;
    }

    /** {@inheritDoc} */
    @Override public IgnitePredicate<ClusterNode> getNodeFilter() {
        return nodeFilter;
    }

    /**
     * @param ctx Context.
     */
    public void context(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"RedundantIfStatement", "EqualsWhichDoesntCheckParameterClass"})
    @Override public boolean equals(Object o) {
        if (!equalsIgnoreNodeFilter(o))
            return false;

        ServiceConfiguration that = (ServiceConfiguration)o;

        if (nodeFilter != null && that.getNodeFilter() != null) {
            if (!nodeFilter.getClass().equals(that.getNodeFilter().getClass()))
                return false;
        }
        else if (nodeFilter != null || that.getNodeFilter() != null)
            return false;

        return true;
    }

    /** {@inheritDoc} */
    // TODO review.
    @SuppressWarnings("RedundantIfStatement")
    @Override public boolean equalsIgnoreNodeFilter(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ServiceConfiguration that = (ServiceConfiguration)o;

        if (maxPerNodeCnt != that.getMaxPerNodeCount())
            return false;

        if (totalCnt != that.getTotalCount())
            return false;

        if (affKey != null ? !affKey.equals(that.getAffinityKey()) : that.getAffinityKey() != null)
            return false;

        if (cacheName != null ? !cacheName.equals(that.getCacheName()) : that.getCacheName() != null)
            return false;

        if (name != null ? !name.equals(that.getName()) : that.getName() != null)
            return false;

//        Service srvc = that.getService();
//
//        if (svc != null ? !svc.getClass().equals(srvc.getClass()) : srvc != null)
//            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return name == null ? 0 : name.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        String svcCls = svc == null ? "" : svc.getClass().getSimpleName();
        String nodeFilterCls = nodeFilter == null ? "" : nodeFilter.getClass().getSimpleName();

        return S.toString(LazyServiceConfiguration.class, this, "svcCls", svcCls, "nodeFilterCls", nodeFilterCls);
    }
}
