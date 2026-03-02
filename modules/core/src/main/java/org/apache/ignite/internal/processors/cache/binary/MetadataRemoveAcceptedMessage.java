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

package org.apache.ignite.internal.processors.cache.binary;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Acknowledge message for {@link MetadataRemoveProposedMessage}: see its javadoc for detailed description of protocol.
 * <p>
 * As discovery messaging doesn't guarantee that message makes only one pass across the cluster
 * <b>MetadataRemoveAcceptedMessage</b> enables to mark it as duplicated so other nodes won't process it but skip.
 */
public class MetadataRemoveAcceptedMessage implements DiscoveryCustomMessage, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(0)
    IgniteUuid id;

    /** */
    @Order(1)
    int typeId;

    /** */
    @Order(2)
    boolean duplicated;

    /** Constructor. */
    public MetadataRemoveAcceptedMessage() {
        // No-op.
    }

    /**
     * @param typeId Type id.
     */
    MetadataRemoveAcceptedMessage(int typeId) {
        id = IgniteUuid.randomUuid();
        this.typeId = typeId;
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
    @Nullable @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr,
        AffinityTopologyVersion topVer, DiscoCache discoCache) {
        throw new UnsupportedOperationException();
    }

    /** */
    public int typeId() {
        return typeId;
    }

    /** */
    public boolean duplicated() {
        return duplicated;
    }

    /**
     * @param duplicated duplicated flag.
     */
    public void duplicated(boolean duplicated) {
        this.duplicated = duplicated;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetadataRemoveAcceptedMessage.class, this);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 502;
    }
}
