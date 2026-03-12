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

package org.apache.ignite.internal.managers.encryption;

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

/**
 * Change cache group encryption key request.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class ChangeCacheEncryptionRequest implements Message {
    /** Request ID. */
    @Order(0)
    UUID reqId;

    /** Cache group IDs. */
    @Order(1)
    int[] grpIds;

    /** Encryption keys. */
    @Order(2)
    byte[][] keys;

    /** Key identifiers. */
    @Order(3)
    byte[] keyIds;

    /** Master key digest. */
    @Order(4)
    byte[] masterKeyDigest;

    /** Default constructor for {@link MessageFactory}. */
    public ChangeCacheEncryptionRequest() {
        // No-op.
    }

    /**
     * @param grpIds Cache group IDs.
     * @param keys Encryption keys.
     * @param keyIds Key identifiers.
     * @param masterKeyDigest Master key digest.
     */
    public ChangeCacheEncryptionRequest(int[] grpIds, byte[][] keys, byte[] keyIds, byte[] masterKeyDigest) {
        this.reqId = UUID.randomUUID();
        this.grpIds = grpIds;
        this.keys = keys;
        this.keyIds = keyIds;
        this.masterKeyDigest = masterKeyDigest;
    }

    /**
     * @return Request ID.
     */
    public UUID requestId() {
        return reqId;
    }

    /**
     * @return Cache group IDs.
     */
    public int[] groupIds() {
        return grpIds;
    }

    /**
     * @return Encryption keys.
     */
    public byte[][] keys() {
        return keys;
    }

    /**
     * @return Key identifiers.
     */
    public byte[] keyIds() {
        return keyIds;
    }

    /**
     * @return Master key digest.
     */
    public byte[] masterKeyDigest() {
        return masterKeyDigest;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        return Objects.equals(reqId, ((ChangeCacheEncryptionRequest)o).reqId);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 38;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(reqId);
    }
}
