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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

/** Message for {@link LazyServiceConfiguration}. */
public class LazyServiceConfigurationMessage implements MarshallableMessage {
    /** Service name. */
    @Order(0)
    String name;

    /** Total count. */
    @Order(1)
    int totalCnt;

    /** Max per-node count. */
    @Order(2)
    int maxPerNodeCnt;

    /** Cache name. */
    @Order(3)
    String cacheName;

    /** Affinity key. */
    private Object affKey;

    /** Serialized {@link #affKey}. */
    @Order(4)
    byte[] affKeyBytes;

    /** Enables or disables service statistics. */
    @Order(5)
    boolean isStatisticsEnabled;

    /** Node local start order. */
    @Order(6)
    int locStartOrder;

    /** Service class name. */
    @Order(7)
    String srvcClsName;

    /** Serialized service. */
    @Order(8)
    byte[] srvcBytes;

    /** Serialized node filter. */
    @Order(9)
    byte[] nodeFilterBytes;

    /** Serialized interceptors. */
    @Order(10)
    byte[] interceptorsBytes;

    /** Names of platform service methods to build service statistics. */
    @Order(11)
    @GridToStringExclude
    String[] platformMtdNames;

    /** Default constructor for {@link MessageFactory}. */
    public LazyServiceConfigurationMessage() {
        // No-op.
    }

    /** @param cfg Service config. */
    public LazyServiceConfigurationMessage(LazyServiceConfiguration cfg) {
        assert cfg != null : "LazyServiceConfiguration is null";

        name = cfg.getName();
        totalCnt = cfg.getTotalCount();
        maxPerNodeCnt = cfg.getMaxPerNodeCount();
        cacheName = cfg.getCacheName();
        affKey = cfg.getAffinityKey();
        isStatisticsEnabled = cfg.isStatisticsEnabled();
        locStartOrder = cfg.getLocalStartOrder();
        srvcClsName = cfg.serviceClassName();
        srvcBytes = cfg.serviceBytes();
        nodeFilterBytes = cfg.nodeFilterBytes();
        interceptorsBytes = cfg.interceptorBytes();
        platformMtdNames = cfg.platformMtdNames();
    }

    /** @return Service configuration. */
    public LazyServiceConfiguration toConfiguration() {
        LazyServiceConfiguration cfg = (LazyServiceConfiguration)new LazyServiceConfiguration()
            .setName(name)
            .setTotalCount(totalCnt)
            .setMaxPerNodeCount(maxPerNodeCnt)
            .setCacheName(cacheName)
            .setAffinityKey(affKey)
            .setStatisticsEnabled(isStatisticsEnabled)
            .setLocalStartOrder(locStartOrder);

        return cfg.serviceClassName(srvcClsName)
            .serviceBytes(srvcBytes)
            .nodeFilterBytes(nodeFilterBytes)
            .interceptorBytes(interceptorsBytes)
            .platformMtdNames(platformMtdNames);
    }

    /** {@inheritDoc} */
    @Override public void marshal(Marshaller marsh) throws IgniteCheckedException {
        if (affKey != null && affKeyBytes == null)
            affKeyBytes = U.marshal(marsh, affKey);
    }

    /** {@inheritDoc} */
    @Override public void unmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        if (!F.isEmpty(affKeyBytes)) {
            affKey = U.unmarshal(marsh, affKeyBytes, clsLdr);

            affKeyBytes = null;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(LazyServiceConfigurationMessage.class, this);
    }
}
