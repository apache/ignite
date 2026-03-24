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

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

/** Master key change request. */
public class MasterKeyChangeRequest implements Message {
    /** Request id. */
    @Order(0)
    UUID reqId;

    /** Encrypted master key name. */
    @Order(1)
    byte[] encKeyName;

    /** Master key digest. */
    @Order(2)
    byte[] digest;

    /** Default constructor for {@link MessageFactory}. */
    public MasterKeyChangeRequest() {
        // No-op.
    }

    /**
     * @param reqId Request id.
     * @param encKeyName Encrypted master key name.
     * @param digest Master key digest.
     */
    public MasterKeyChangeRequest(UUID reqId, byte[] encKeyName, byte[] digest) {
        this.reqId = reqId;
        this.encKeyName = encKeyName;
        this.digest = digest;
    }

    /** @return Request id. */
    UUID requestId() {
        return reqId;
    }

    /** @return Encrypted master key name. */
    byte[] encKeyName() {
        return encKeyName;
    }

    /** @return Master key digest. */
    byte[] digest() {
        return digest;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof MasterKeyChangeRequest))
            return false;

        MasterKeyChangeRequest key = (MasterKeyChangeRequest)o;

        return Arrays.equals(encKeyName, key.encKeyName) &&
            Arrays.equals(digest, key.digest) &&
            Objects.equals(reqId, key.reqId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = Objects.hash(reqId);

        res = 31 * res + Arrays.hashCode(encKeyName);
        res = 31 * res + Arrays.hashCode(digest);

        return res;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 35;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MasterKeyChangeRequest.class, this);
    }
}
