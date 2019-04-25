/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/** */
class DistributedMetaStorageCasMessage extends DistributedMetaStorageUpdateMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final byte[] expectedVal;

    /** */
    private boolean matches = true;

    /** */
    public DistributedMetaStorageCasMessage(UUID reqId, String key, byte[] expValBytes, byte[] valBytes) {
        super(reqId, key, valBytes);

        expectedVal = expValBytes;
    }

    /** */
    public byte[] expectedValue() {
        return expectedVal;
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
        return new DistributedMetaStorageCasAckMessage(requestId(), errorMessage(), matches);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DistributedMetaStorageCasMessage.class, this, super.toString());
    }
}
