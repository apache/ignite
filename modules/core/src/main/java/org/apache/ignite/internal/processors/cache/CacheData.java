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

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.Marshalled;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.query.QuerySchema;
import org.apache.ignite.internal.processors.query.schema.message.QueryEntityMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

/** Cache information sent in discovery data to joining node. */
public class CacheData implements Message {
    /** */
    @Marshalled("cacheCfgBytes")
    CacheConfiguration cacheCfg;

    /** Serialized {@link #cacheCfg}. */
    @Order(0)
    byte[] cacheCfgBytes;

    /** */
    @Order(1)
    int grpId;

    /** */
    @Order(2)
    CacheType cacheType;

    /** */
    @Order(3)
    IgniteUuid deploymentId;

    /** */
    private Collection<QueryEntity> entities;

    /** */
    @Order(4)
    Collection<QueryEntityMessage> entitiesMsgs;

    /** */
    @Order(5)
    UUID rcvdFrom;

    /** */
    @Order(6)
    boolean staticCfg;

    /** */
    @Order(7)
    boolean sql;

    /** Cache configuration enrichment. */
    @Order(8)
    CacheConfigurationEnrichment cacheCfgEnrichment;

    /** Default constructor for {@link MessageFactory}. */
    public CacheData() {
        // No-op.
    }

    /**
     * @param cacheCfg Cache configuration.
     * @param grpId Cache group ID.
     * @param cacheType Cache type.
     * @param deploymentId Cache deployment ID.
     * @param schema Query schema.
     * @param rcvdFrom Node ID cache was started from.
     * @param staticCfg {@code True} if cache was statically configured.
     * @param sql {@code True} if cache was created by an SQL command such as {@code CREATE TABLE}.
     * @param template {@code True} if this is cache template.
     * @param cacheCfgEnrichment Cache configuration enrichment.
     */
    CacheData(CacheConfiguration cacheCfg,
        int grpId,
        CacheType cacheType,
        IgniteUuid deploymentId,
        QuerySchema schema,
        UUID rcvdFrom,
        boolean staticCfg,
        boolean sql,
        boolean template,
        CacheConfigurationEnrichment cacheCfgEnrichment
    ) {
        assert cacheCfg != null;
        assert rcvdFrom != null : cacheCfg.getName();
        assert deploymentId != null : cacheCfg.getName();
        assert template || grpId != 0 : cacheCfg.getName();

        this.cacheCfg = cacheCfg;
        this.grpId = grpId;
        this.cacheType = cacheType;
        this.deploymentId = deploymentId;
        entitiesMsgs = F.viewReadOnly(schema.entities(), QueryEntityMessage::new);
        this.rcvdFrom = rcvdFrom;
        this.staticCfg = staticCfg;
        this.sql = sql;
        this.cacheCfgEnrichment = cacheCfgEnrichment;
    }

    /** @return Cache group ID. */
    public int groupId() {
        return grpId;
    }

    /** @return Cache type. */
    public CacheType cacheType() {
        return cacheType;
    }

    /** @return Start ID. */
    public IgniteUuid deploymentId() {
        return deploymentId;
    }

    /** @return {@code True} if statically configured. */
    public boolean staticallyConfigured() {
        return staticCfg;
    }

    /** @return {@code True} if cache was created by an SQL command such as {@code CREATE TABLE}. */
    public boolean sql() {
        return sql;
    }

    /** @return Cache configuration. */
    public CacheConfiguration cacheConfiguration() {
        return cacheCfg;
    }

    /** @return Schema. */
    public QuerySchema schema() {
        if (entities == null)
            entities = F.transform(entitiesMsgs, QueryEntityMessage::toEntity);

        return new QuerySchema(entities);
    }

    /** @return ID of node provided cache configuration. */
    public UUID receivedFrom() {
        return rcvdFrom;
    }

    /** @return Cache configuration enrichment. */
    public CacheConfigurationEnrichment cacheConfigurationEnrichment() {
        return cacheCfgEnrichment;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheData.class, this, "cacheName", cacheCfg.getName());
    }
}
