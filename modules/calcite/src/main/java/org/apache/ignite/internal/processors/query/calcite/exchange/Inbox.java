/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
 * TODO
 */
public class Inbox<T> implements SingleNode<T> {
    private final GridCacheVersion queryId;
    private final long exchangeId;

    private Sink<T> target;
    private Collection<UUID> sources;
    private Comparator<T> comparator;
    private ExchangeProcessor srvc;

    public Inbox(GridCacheVersion queryId, long exchangeId) {
        this.queryId = queryId;
        this.exchangeId = exchangeId;
    }

    public void bind(Sink<T> target, Collection<UUID> sources, Comparator<T> comparator) {
        this.target = target;
        this.sources = sources;
        this.comparator = comparator;
    }

    void init(ExchangeProcessor srvc) {
        this.srvc = srvc;
    }

    @Override public void signal() {
        // No-op.
    }

    @Override public void sources(List<Source> sources) {
        throw new UnsupportedOperationException();
    }

    @Override public Sink<T> sink(int idx) {
        throw new UnsupportedOperationException();
    }

    public void push(UUID source, int batchId, List<?> rows) {

    }
}
