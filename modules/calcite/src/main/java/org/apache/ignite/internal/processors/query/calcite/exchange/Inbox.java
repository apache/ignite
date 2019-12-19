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

package org.apache.ignite.internal.processors.query.calcite.exchange;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.calcite.exec.SingleNode;
import org.apache.ignite.internal.processors.query.calcite.exec.Sink;
import org.apache.ignite.internal.processors.query.calcite.exec.Source;

/**
 * TODO https://issues.apache.org/jira/browse/IGNITE-12448
 */
public class Inbox<T> implements SingleNode<T> {
    /** */
    private final GridCacheVersion queryId;

    /** */
    private final long exchangeId;

    /** */
    private Sink<T> target;

    /** */
    private Collection<UUID> sources;

    /** */
    private Comparator<T> comparator;

    /** */
    private ExchangeProcessor srvc;

    /**
     * @param queryId Query ID.
     * @param exchangeId Exchange ID.
     */
    public Inbox(GridCacheVersion queryId, long exchangeId) {
        this.queryId = queryId;
        this.exchangeId = exchangeId;
    }

    /**
     * Binds this Inbox to the given target.
     *
     * @param target Target sink.
     * @param sources Source nodes.
     * @param comparator Optional comparator for merge exchange.
     */
    public void bind(Sink<T> target, Collection<UUID> sources, Comparator<T> comparator) {
        this.target = target;
        this.sources = sources;
        this.comparator = comparator;
    }

    /**
     * @param srvc Exchange service.
     */
    void init(ExchangeProcessor srvc) {
        this.srvc = srvc;
    }

    /** {@inheritDoc} */
    @Override public void signal() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void sources(List<Source> sources) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Sink<T> sink(int idx) {
        throw new UnsupportedOperationException();
    }

    /**
     * Pushes a batch into a buffer.
     *
     * @param source Source node.
     * @param batchId Batch ID.
     * @param rows Rows.
     */
    public void push(UUID source, int batchId, List<?> rows) {

    }
}
