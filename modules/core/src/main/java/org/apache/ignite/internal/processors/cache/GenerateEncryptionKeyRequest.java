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
import java.util.Map;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Generate encryption key request.
 */
public class GenerateEncryptionKeyRequest implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Discovery custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    private Collection<Integer> grpIds;

    /** */
    private Map<Integer, byte[]> encGrpKeys;

    public GenerateEncryptionKeyRequest(Collection<Integer> grpIds) {
        this.grpIds = grpIds;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean stopProcess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        return null;
    }

    /**
     * @return Group ids for a key generation.
     */
    public Collection<Integer> grpIds() {
        return grpIds;
    }

    /**
     * @return Encrypted group keys.
     */
    public Map<Integer, byte[]> encGrpKeys() {
        return encGrpKeys;
    }

    /**
     * @param encGrpKeys Encrypted group keys.
     */
    public void encGrpKeys(Map<Integer, byte[]> encGrpKeys) {
        this.encGrpKeys = encGrpKeys;
    }
}
