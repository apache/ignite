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

import java.util.Collection;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Generate encryption key response.
 */
public class GenerateEncryptionKeyResponse implements Message {
    /** Request message ID. */
    @Order(value = 0, method = "requestId")
    private IgniteUuid id;

    /** */
    @Order(value = 1, method = "encryptionKeys")
    private Collection<byte[]> encKeys;

    /** Master key digest that encrypted group encryption keys. */
    @Order(2)
    private byte[] masterKeyDigest;

    /** */
    public GenerateEncryptionKeyResponse() {
    }

    /**
     * @param id Request id.
     * @param encKeys Encryption keys.
     * @param masterKeyDigest Master key digest.
     */
    public GenerateEncryptionKeyResponse(IgniteUuid id, Collection<byte[]> encKeys, byte[] masterKeyDigest) {
        this.id = id;
        this.encKeys = encKeys;
        this.masterKeyDigest = masterKeyDigest;
    }

    /**
     * @return Request id.
     */
    public IgniteUuid requestId() {
        return id;
    }

    /**
     * @param id Request id.
     */
    public void requestId(IgniteUuid id) {
        this.id = id;
    }

    /**
     * @return Encryption keys.
     */
    public Collection<byte[]> encryptionKeys() {
        return encKeys;
    }

    /**
     * @param encKeys Encryption keys.
     */
    public void encryptionKeys(Collection<byte[]> encKeys) {
        this.encKeys = encKeys;
    }

    /** @return Master key digest that encrypted group encryption keys. */
    public byte[] masterKeyDigest() {
        return masterKeyDigest;
    }

    /**
     * @param masterKeyDigest Master key digest that encrypted group encryption keys.
     */
    public void masterKeyDigest(byte[] masterKeyDigest) {
        this.masterKeyDigest = masterKeyDigest;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 163;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GenerateEncryptionKeyResponse.class, this);
    }
}
