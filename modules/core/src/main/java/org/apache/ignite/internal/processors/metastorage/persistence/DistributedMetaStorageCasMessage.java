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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.jetbrains.annotations.Nullable;

/** */
public class DistributedMetaStorageCasMessage extends DistributedMetaStorageUpdateMessage {
    /** */
    private @Nullable Serializable expVal;

    /** */
    @Order(0)
    byte[] expValBytes;

    /** */
    @Order(1)
    boolean matches;

    /** Empty constructor for {@link MessageFactory}. */
    public DistributedMetaStorageCasMessage() {
        // No-op.
    }

    /** */
    public DistributedMetaStorageCasMessage(UUID reqId, String key, @Nullable Serializable expVal, @Nullable Serializable val) {
        super(reqId, key, val);

        this.expVal = expVal;
        matches = true;
    }

    /** */
    public Serializable expectedValue() {
        return expVal;
    }

    /** */
    public void setMatches(boolean matches) {
        this.matches = matches;
    }

    /** */
    public boolean matches() {
        return matches;
    }

    /** {@inheritDoc} */
    @Override @Nullable public DiscoveryCustomMessage ackMessage() {
        return new DistributedMetaStorageCasAckMessage(reqId, matches);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        super.prepareMarshal(marsh);

        if (expVal != null && expValBytes == null)
            expValBytes = U.marshal(marsh, expVal);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh) throws IgniteCheckedException {
        super.finishUnmarshal(marsh);

        if (expValBytes != null && expVal == null)
            expVal = U.unmarshal(marsh, expValBytes, U.gridClassLoader());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DistributedMetaStorageCasMessage.class, this, super.toString());
    }
}
