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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Represents a single page with results of cache query from single node.
 */
public class NodePage<R> implements Iterator<R> {
    /** Iterator over data in this page. */
    private final Iterator<R> pageIter;

    /** Node ID. */
    private final UUID nodeId;

    /** Head of page. Requires {@link #hasNext()} to be called on {@link #pageIter} before. */
    private R head;

    /**
     * @param nodeId Node ID.
     * @param data Page data.
     */
    public NodePage(UUID nodeId, Collection<R> data) {
        pageIter = data.iterator();
        this.nodeId = nodeId;
    }

    /** */
    public R head() {
        return head;
    }

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        if (head != null)
            return true;
        else {
            if (!pageIter.hasNext())
                return false;
            else
                return (head = pageIter.next()) != null;
        }
    }

    /** {@inheritDoc} */
    @Override public R next() {
        if (head == null)
            throw new NoSuchElementException("Node page is empty.");

        R o = head;

        head = null;

        return o;
    }
}
