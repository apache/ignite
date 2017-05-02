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
import java.util.Set;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

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
    private final CacheConfiguration cacheCfg;

    /** */
    private final AffinityTopologyVersion startTopVer;

    /** */
    @GridToStringInclude
    private final Set<String> cacheNames;

    /**
     * @param cacheCfg Cache configuration.
     * @param grpId
     * @param startTopVer
     */
    public CacheGroupData(CacheConfiguration cacheCfg,
        String grpName,
        int grpId,
        AffinityTopologyVersion startTopVer,
        Set<String> cacheNames) {
        assert cacheCfg != null;
        assert grpName != null;
        assert grpId != 0;
        assert startTopVer != null;

        this.cacheCfg = cacheCfg;
        this.grpName = grpName;
        this.grpId = grpId;
        this.startTopVer = startTopVer;
        this.cacheNames = cacheNames;
    }

    public String groupName() {
        return grpName;
    }

    public int groupId() {
        return grpId;
    }

    public CacheConfiguration config() {
        return cacheCfg;
    }

    public AffinityTopologyVersion startTopologyVersion() {
        return startTopVer;
    }

    Set<String> cacheNames() {
        return cacheNames;
    }

    @Override public String toString() {
        return S.toString(CacheGroupData.class, this);
    }
}
