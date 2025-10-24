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

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Generate encryption key request.
 */
public class GenerateEncryptionKeyRequest implements Message {
    /** Request ID. */
    @Order(0)
    private IgniteUuid id;

    /** */
    @Order(value = 1, method = "keyCount")
    private int keyCnt;

    /** */
    public GenerateEncryptionKeyRequest() {
    }

    /**
     * @param keyCnt Count of encryption key to generate.
     */
    public GenerateEncryptionKeyRequest(int keyCnt) {
        this.keyCnt = keyCnt;
        id = IgniteUuid.randomUuid();
    }

    /**
     * @return Request id.
     */
    public IgniteUuid id() {
        return id;
    }

    /**
     * @param id New request ID.
     */
    public void id(IgniteUuid id) {
        this.id = id;
    }

    /**
     * @return Count of encryption key to generate.
     */
    public int keyCount() {
        return keyCnt;
    }

    /**
     * @param keyCnt New key count.
     */
    public void keyCount(int keyCnt) {
        this.keyCnt = keyCnt;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 162;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GenerateEncryptionKeyRequest.class, this);
    }
}
