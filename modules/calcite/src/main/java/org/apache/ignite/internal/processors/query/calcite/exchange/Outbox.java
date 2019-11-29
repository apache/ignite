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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.calcite.exec.AbstractNode;
import org.apache.ignite.internal.processors.query.calcite.exec.SingleNode;
import org.apache.ignite.internal.processors.query.calcite.exec.Sink;
import org.apache.ignite.internal.processors.query.calcite.trait.DestinationFunction;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class Outbox<T> extends AbstractNode<T> implements SingleNode<T>, Sink<T> {
    static final int BATCH_SIZE = 200;
    static final int PER_NODE_BATCH_COUNT = 10;

    private final Map<UUID, Destination> perNode = new HashMap<>();

    private final GridCacheVersion queryId;
    private final long exchangeId;
    private final Collection<UUID> targets;
    private final DestinationFunction function;

    private ExchangeService srvc;

    protected Outbox(GridCacheVersion queryId, long exchangeId, Collection<UUID> targets, DestinationFunction function) {
        super(Sink.noOp());
        this.queryId = queryId;
        this.exchangeId = exchangeId;

        this.targets = targets;
        this.function = function;
    }

    public void acknowledge(UUID nodeId, int batchId) {
        perNode.get(nodeId).acknowledge(batchId);
    }

    @Override public Sink<T> sink(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    @Override public boolean push(T row) {
        List<UUID> nodes = function.destination(row);

        if (F.isEmpty(nodes))
            return true;

        List<Destination> destinations = new ArrayList<>(nodes.size());

        for (UUID node : nodes) {
            Destination dest = perNode.computeIfAbsent(node, Destination::new);

            if (!dest.ready()) {
                dest.needSignal();

                return false;
            }

            destinations.add(dest);
        }

        for (Destination dest : destinations)
            dest.add(row);

        return true;
    }

    public void init(ExchangeService srvc) {
        this.srvc = srvc;

        srvc.register(this);

        signal();
    }

    @Override public void end() {
        for (UUID node : targets)
            perNode.computeIfAbsent(node, Destination::new).end();

        srvc.unregister(this);
    }

    private final class Destination {
        private final UUID nodeId;

        private int hwm = -1;
        private int lwm = -1;

        private ArrayList<Object> curr = new ArrayList<>(BATCH_SIZE + 1); // extra space for end marker;

        private boolean needSignal;

        private Destination(UUID nodeId) {
            this.nodeId = nodeId;
        }

        public void add(T row) {
            if (curr.size() == BATCH_SIZE) {
                assert ready() && srvc != null;

                srvc.send(queryId, exchangeId, nodeId, ++hwm, curr);

                curr = new ArrayList<>(BATCH_SIZE);
            }

            curr.add(row);
        }

        public void end() {
            curr.add(EndMarker.INSTANCE);

            assert srvc != null;

            srvc.send(queryId, exchangeId, nodeId, hwm, curr);

            curr = null;
            hwm = Integer.MAX_VALUE;
        }

        boolean ready() {
            return hwm - lwm < PER_NODE_BATCH_COUNT || curr.size() < BATCH_SIZE;
        }

        void acknowledge(int id) {
            if (lwm < id) {
                lwm = id;

                if (needSignal) {
                    needSignal = false;

                    signal();
                }
            }
        }

        public void needSignal() {
            needSignal = true;
        }
    }
}
