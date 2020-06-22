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

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Continuous query handle.
 */
@SuppressWarnings("rawtypes")
public class ClientCacheQueryContinuousHandle implements CacheEntryUpdatedListener<Object, Object> {
    /** */
    private final ClientConnectionContext ctx;

    /** */
    private final Lock modeLock = new ReentrantLock();

    /** */
    private QueryCursor cursor;

    /** */
    private long cursorId;

    /** Queue to store events while we wait for cursorId to be available. */
    private Queue<CacheEntryEvent> eventBuffer;

    /**
     * Ctor.
     * @param ctx Context.
     */
    public ClientCacheQueryContinuousHandle(ClientConnectionContext ctx) {
        assert ctx != null;
        assert cursor != null;

        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> iterable) throws CacheEntryListenerException {
        modeLock.lock();

        try {
            if (cursor == null) {
                // We can't send events to the client until the client has the cursor id. Buffer them.
                if (eventBuffer == null)
                    eventBuffer = new ConcurrentLinkedQueue<>();

                for (Object e : iterable)
                    eventBuffer.add((CacheEntryEvent) e);
            } else {
                // TODO: Include iterable
                ctx.notifyClient(new ClientNotification(ClientMessageParser.OP_QUERY_CONTINUOUS_EVENT_NOTIFICATION, cursorId));
            }
        } finally {
            modeLock.unlock();
        }
    }

    /**
     * Sets the cursor id.
     * @param cursor Cursor.
     * @param cursorId Cursor id.
     */
    public void setCursor(QueryCursor cursor, long cursorId) {
        modeLock.lock();

        try {
            this.cursor = cursor;
            this.cursorId = cursorId;

            // TODO: Send buffered events
        } finally {
            modeLock.unlock();
        }
    }
}
