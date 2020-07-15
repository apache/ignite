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

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.processors.platform.client.ClientCloseableResource;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientMessageParser;

/**
 * Continuous query handle.
 * NOTE: Do not mark with {@link org.apache.ignite.lang.IgniteAsyncCallback} - it disables batching and sends
 * events one by one.
 */
public class ClientCacheQueryContinuousHandle implements CacheEntryUpdatedListener<Object, Object>, ClientCloseableResource {
    /** */
    private final ClientConnectionContext ctx;

    /** */
    private volatile Long id;

    /** */
    private volatile QueryCursor<?> cur;

    /**
     * Ctor.
     * @param ctx Context.
     */
    public ClientCacheQueryContinuousHandle(ClientConnectionContext ctx) {
        assert ctx != null;

        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> iterable) throws CacheEntryListenerException {
        // Client is not yet ready to receive notifications - skip them.
        if (id == null)
            return;

        ClientCacheEntryEventNotification notification = new ClientCacheEntryEventNotification(
                ClientMessageParser.OP_QUERY_CONTINUOUS_EVENT_NOTIFICATION, id, iterable);

        ctx.notifyClient(notification);
    }

    /**
     * Sets the cursor id.
     * @param id Cursor id.
     */
    public void startNotifications(long id, QueryCursor<?> cur) {
        assert cur != null;

        this.id = id;
        this.cur = cur;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        QueryCursor<?> cur0 = cur;

        if (cur0 != null)
        {
            cur0.close();
            cur = null;
        }
    }
}
