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

package org.apache.ignite.internal.managers.failover;

import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.loadbalancer.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.failover.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * GridFailoverContext implementation.
 */
public class GridFailoverContextImpl implements FailoverContext {
    /** Grid task session. */
    private final GridTaskSessionImpl taskSes;

    /** Failed job result. */
    private final ComputeJobResult jobRes;

    /** Load balancing manager. */
    @GridToStringExclude
    private final GridLoadBalancerManager loadMgr;

    /** Affinity key for affinityCall. */
    private final Object affKey;

    /** Affinity cache name for affinityCall. */
    private final String affCacheName;

    /**
     * Initializes failover context.
     *
     * @param taskSes Grid task session.
     * @param jobRes Failed job result.
     * @param loadMgr Load manager.
     * @param affKey Affinity key.
     * @param affCacheName Affinity cache name.
     */
    public GridFailoverContextImpl(GridTaskSessionImpl taskSes,
        ComputeJobResult jobRes,
        GridLoadBalancerManager loadMgr,
        @Nullable Object affKey,
        @Nullable String affCacheName) {
        assert taskSes != null;
        assert jobRes != null;
        assert loadMgr != null;

        this.taskSes = taskSes;
        this.jobRes = jobRes;
        this.loadMgr = loadMgr;
        this.affKey = affKey;
        this.affCacheName = affCacheName;
    }

    /** {@inheritDoc} */
    @Override public ComputeTaskSession getTaskSession() {
        return taskSes;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResult getJobResult() {
        return jobRes;
    }

    /** {@inheritDoc} */
    @Override public ClusterNode getBalancedNode(List<ClusterNode> top) {
        return loadMgr.getBalancedNode(taskSes, top, jobRes.getJob());
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object affinityKey() {
        return affKey;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String affinityCacheName() {
        return affCacheName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridFailoverContextImpl.class, this);
    }
}
