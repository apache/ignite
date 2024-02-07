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

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Cache data manipulation request.
 */
abstract class ClientCacheDataRequest extends ClientCacheRequest {
    /** Transaction ID. Only available if request was made under a transaction. */
    private final int txId;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    ClientCacheDataRequest(BinaryRawReader reader) {
        super(reader);

        txId = isTransactional() ? reader.readInt() : 0;
    }

    /**
     * Gets transaction ID.
     */
    public int txId() {
        return txId;
    }

    /** {@inheritDoc} */
    @Override public boolean isTransactional() {
        return super.isTransactional();
    }

    /** Chain cache operation future to return response when operation is completed. */
    protected static <T> IgniteInternalFuture<ClientResponse> chainFuture(
        IgniteFuture<T> fut,
        IgniteClosure<T, ClientResponse> clo
    ) {
        // IgniteFuture for cache operations executes chaining/listening block via task to external executor,
        // we don't need this additional step here, so use internal future.
        IgniteInternalFuture<T> fut0 = ((IgniteFutureImpl<T>)fut).internalFuture();

        return fut0.chain(f -> {
            try {
                return clo.apply(f.get());
            }
            catch (Exception e) {
                throw new GridClosureException(e);
            }
        });
    }
}
