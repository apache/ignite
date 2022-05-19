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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Mutable message verifies that every node in cluster doesn't run another instance of Consistent Cut procedure.
 */
public class ConsistentCutCheckDiscoveryMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /**
     * Whether the Consistent Cut procedure is in progress on any node within a cluster.
     */
    @GridToStringInclude
    private boolean inProgress;

    /**
     * @return Whether found a node is currently running Consistent Cut procedure.
     */
    public boolean progress() {
        return inProgress;
    }

    /**
     * Notify that current node is running Consistent Cut procedure.
     */
    public void inProgress() {
        inProgress = true;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /**
     * Invoked on coordinator after every node was checked.
     */
    @Override public @Nullable DiscoveryCustomMessage ackMessage() {
        if (!progress())
            return new ConsistentCutStartDiscoveryMessage();

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        return null;
    }

    /** */
    @Override public String toString() {
        return S.toString(ConsistentCutCheckDiscoveryMessage.class, this);
    }
}
