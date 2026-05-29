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

package org.apache.ignite.internal.client.thin;

import java.util.UUID;
import org.apache.ignite.client.ClientServiceDescriptor;
import org.apache.ignite.platform.PlatformType;
import org.apache.ignite.services.Service;
import org.jetbrains.annotations.Nullable;

/**
 * Descriptor of {@link Service}.
 */
class ClientServiceDescriptorImpl implements ClientServiceDescriptor {
    /** */
    private final String name;

    /** */
    private final String svcCls;

    /** */
    private final int totalCnt;

    /** */
    private final int maxPerNodeCnt;

    /** */
    private final String cacheName;

    /** */
    private final UUID originNodeId;

    /** */
    private final PlatformType platformType;

    /** */
    ClientServiceDescriptorImpl(
        String name,
        String svcCls,
        int totalCnt,
        int maxPerNodeCnt,
        String cacheName,
        UUID originNodeId,
        PlatformType platformType
    ) {
        this.name = name;
        this.svcCls = svcCls;
        this.totalCnt = totalCnt;
        this.maxPerNodeCnt = maxPerNodeCnt;
        this.cacheName = cacheName;
        this.originNodeId = originNodeId;
        this.platformType = platformType;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String serviceClass() {
        return svcCls;
    }

    /** {@inheritDoc} */
    @Override public int totalCount() {
        return totalCnt;
    }

    /** {@inheritDoc} */
    @Override public int maxPerNodeCount() {
        return maxPerNodeCnt;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public UUID originNodeId() {
        return originNodeId;
    }

    /** {@inheritDoc} */
    @Override public PlatformType platformType() {
        return platformType;
    }
}
