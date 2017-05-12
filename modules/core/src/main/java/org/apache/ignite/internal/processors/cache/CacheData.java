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
import java.util.UUID;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.QuerySchema;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Cache information sent in discovery data to joining node.
 */
public class CacheData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final CacheConfiguration cacheCfg;

    /** */
    private final Integer cacheId;

    /** */
    private final CacheType cacheType;

    /** */
    private final IgniteUuid deploymentId;

    /** */
    private final QuerySchema schema;

    /** */
    private final UUID rcvdFrom;

    /** */
    private final boolean staticCfg;

    /** */
    private final boolean template;

    /** Flags added for future usage. */
    private final byte flags;

    /**
     * @param cacheCfg Cache configuration.
     * @param cacheId Cache ID.
     * @param cacheType Cache ID.
     * @param deploymentId Cache deployment ID.
     * @param schema Query schema.
     * @param rcvdFrom Node ID cache was started from.
     * @param staticCfg {@code True} if cache was statically configured.
     * @param template {@code True} if this is cache template.
     * @param flags Flags (added for future usage).
     */
    CacheData(CacheConfiguration cacheCfg,
        int cacheId,
        CacheType cacheType,
        IgniteUuid deploymentId,
        QuerySchema schema,
        UUID rcvdFrom,
        boolean staticCfg,
        boolean template,
        byte flags) {
        assert cacheCfg != null;
        assert rcvdFrom != null : cacheCfg.getName();
        assert deploymentId != null : cacheCfg.getName();
        assert template || cacheId != 0 : cacheCfg.getName();

        this.cacheCfg = cacheCfg;
        this.cacheId = cacheId;
        this.cacheType = cacheType;
        this.deploymentId = deploymentId;
        this.schema = schema;
        this.rcvdFrom = rcvdFrom;
        this.staticCfg = staticCfg;
        this.template = template;
        this.flags = flags;
    }

    /**
     * @return Cache ID.
     */
    public Integer cacheId() {
        return cacheId;
    }

    /**
     * @return {@code True} if this is template configuration.
     */
    public boolean template() {
        return template;
    }

    /**
     * @return Cache type.
     */
    public CacheType cacheType() {
        return cacheType;
    }

    /**
     * @return Start ID.
     */
    public IgniteUuid deploymentId() {
        return deploymentId;
    }

    /**
     * @return {@code True} if statically configured.
     */
    public boolean staticallyConfigured() {
        return staticCfg;
    }

    /**
     * @return Cache configuration.
     */
    public CacheConfiguration cacheConfiguration() {
        return cacheCfg;
    }

    /**
     * @return Schema.
     */
    public QuerySchema schema() {
        return schema.copy();
    }

    /**
     * @return ID of node provided cache configuration.
     */
    public UUID receivedFrom() {
        return rcvdFrom;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheData.class, this, "cacheName", cacheCfg.getName());
    }
}
