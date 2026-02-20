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

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/** */
public class DistributedMetaStorageCasMessage extends DistributedMetaStorageUpdateMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(value = 4, method = "expectedValue")
    private byte[] expectedVal;

    /** */
    @Order(5)
    private boolean matches;

    /** Empty constructor for {@link DiscoveryMessageFactory}. */
    public DistributedMetaStorageCasMessage() {
        // No-op.
    }

    /** */
    public DistributedMetaStorageCasMessage(UUID reqId, String key, byte[] expValBytes, byte[] valBytes) {
        super(reqId, key, valBytes);

        expectedVal = expValBytes;
        matches = true;
    }

    /** */
    public byte[] expectedValue() {
        return expectedVal;
    }

    /** */
    public void expectedValue(byte[] expectedVal) {
        this.expectedVal = expectedVal;
    }

    /** */
    public void matches(boolean matches) {
        this.matches = matches;
    }

    /** */
    public boolean matches() {
        return matches;
    }

    /** {@inheritDoc} */
    @Override @Nullable public DiscoveryCustomMessage ackMessage() {
        return new DistributedMetaStorageCasAckMessage(requestId(), matches);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 21;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DistributedMetaStorageCasMessage.class, this, super.toString());
    }
}
