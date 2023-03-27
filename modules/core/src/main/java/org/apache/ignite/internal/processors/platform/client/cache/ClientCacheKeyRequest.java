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

package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.platform.client.tx.ClientTxAwareRequest;

/**
 * Cache request involving key.
 */
public abstract class ClientCacheKeyRequest extends ClientCacheDataRequest implements ClientTxAwareRequest {
    /** Key. */
    private final Object key;

    /**
     * Ctor.
     *
     * @param reader Reader.
     */
    ClientCacheKeyRequest(BinaryRawReaderEx reader) {
        super(reader);

        key = reader.readObjectDetached();
    }

    /**
     * Gets the key.
     *
     * @return Key.
     */
    public Object key() {
        return key;
    }
}
