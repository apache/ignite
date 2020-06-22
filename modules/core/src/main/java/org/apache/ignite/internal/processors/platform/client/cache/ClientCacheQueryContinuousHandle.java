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

import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientMessageParser;
import org.apache.ignite.internal.processors.platform.client.ClientNotification;

import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;

/**
 * Continuous query handle.
 */
@SuppressWarnings("rawtypes")
public class ClientCacheQueryContinuousHandle implements CacheEntryUpdatedListener {
    /** */
    private final ClientConnectionContext ctx;

    /** */
    private final QueryCursor cursor;

    /** */
    private long cursorId;

    public ClientCacheQueryContinuousHandle(ClientConnectionContext ctx, QueryCursor cursor) {
        assert ctx != null;
        assert cursor != null;

        this.ctx = ctx;
        this.cursor = cursor;
        this.cursorId = cursorId;
    }

    /** {@inheritDoc} */
    @Override  public void onUpdated(Iterable iterable) throws CacheEntryListenerException {
        // TODO: Send notification for every item in iterable.
        // TODO: Buffer events while response is not sent.
        ctx.notifyClient(new ClientNotification(ClientMessageParser.OP_QUERY_CONTINUOUS_EVENT_NOTIFICATION, cursorId));
    }
}
