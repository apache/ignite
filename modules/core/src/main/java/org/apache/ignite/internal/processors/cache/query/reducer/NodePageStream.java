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

package org.apache.ignite.internal.processors.cache.query.reducer;

import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryAdapter;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Stream over single node.
 */
public class NodePageStream<R> extends PageStream<R> {
    /** */
    private final UUID nodeId;

    /** */
    private R head;

    /** */
    protected NodePageStream(GridCacheQueryAdapter qry, Object queueLock, long timeoutTime,
        UUID nodeId, BiConsumer<Collection<UUID>, Boolean> reqPages) {
        super(qry, queueLock, timeoutTime, new HashSet<>(F.asList(nodeId)), reqPages);

        this.nodeId = nodeId;
    }

    /**
     * @return Head of stream, that is last item returned with {@link #next()}.
     */
    public R head() {
        return head;
    }

    /**
     * @return Head of stream and then clean it.
     */
    public R get() {
        R ret = head;

        head = null;

        return ret;
    }

    /**
     * @return {@code true} If this stream has next row, {@code false} otherwise.
     */
    @Override public boolean hasNext() throws IgniteCheckedException {
        if (head != null)
            return true;

        return super.hasNext();
    }

    /**
     * @return Next item from this stream.
     */
    @Override public R next() throws IgniteCheckedException {
        head = super.next();

        return head;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }
}
