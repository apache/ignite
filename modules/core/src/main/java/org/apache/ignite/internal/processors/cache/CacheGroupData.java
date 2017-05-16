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

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheGroupData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final String grpName;

    /** */
    private final int grpId;

    /** */
    private final UUID rcvdFrom;

    /** */
    private final IgniteUuid deploymentId;

    /** */
    private final CacheConfiguration cacheCfg;

    /** */
    @GridToStringInclude
    private final Map<String, Integer> caches;

    /**
     * @param cacheCfg Cache configuration.
     * @param grpName Group name.
     * @param grpId  Group ID.
     * @param rcvdFrom Node ID cache group received from.
     * @param deploymentId Deployment ID.
     * @param caches Cache group caches.
     */
    CacheGroupData(
        CacheConfiguration cacheCfg,
        @Nullable String grpName,
        int grpId,
        UUID rcvdFrom,
        IgniteUuid deploymentId,
        Map<String, Integer> caches) {
        assert cacheCfg != null;
        assert grpId > 0 : grpId;
        assert deploymentId != null;

        this.cacheCfg = cacheCfg;
        this.grpName = grpName;
        this.grpId = grpId;
        this.rcvdFrom = rcvdFrom;
        this.deploymentId = deploymentId;
        this.caches = caches;
    }

    public UUID receivedFrom() {
        return rcvdFrom;
    }

    public String groupName() {
        return grpName;
    }

    public int groupId() {
        return grpId;
    }

    public IgniteUuid deploymentId() {
        return deploymentId;
    }

    public CacheConfiguration config() {
        return cacheCfg;
    }

    Map<String, Integer> caches() {
        return caches;
    }

    @Override public String toString() {
        return S.toString(CacheGroupData.class, this);
    }
}
