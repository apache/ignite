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

import java.util.ArrayList;
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
 * TODO https://issues.apache.org/jira/browse/IGNITE-12448
 */
public class Outbox<T> extends AbstractNode<T> implements SingleNode<T>, Sink<T> {
    /** */
    private final Map<UUID, Destination> perNode = new HashMap<>();

    /** */
    private final GridCacheVersion queryId;

    /** */
    private final long exchangeId;

    /** */
    private final DestinationFunction function;

    /** */
    private ExchangeProcessor srvc;

    /**
     *
     * @param queryId Query ID.
     * @param exchangeId Exchange ID.
     * @param function Destination function.
     */
    public Outbox(GridCacheVersion queryId, long exchangeId, DestinationFunction function) {
        super(Sink.noOp());
        this.queryId = queryId;
        this.exchangeId = exchangeId;
        this.function = function;
    }

    public void init(ExchangeProcessor srvc) {
        this.srvc = srvc;

        srvc.register(this);

        signal();
    }

    public void acknowledge(UUID nodeId, int batchId) {
        perNode.get(nodeId).onAcknowledge(batchId);
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

    @Override public void end() {
        for (UUID node : function.targets())
            perNode.computeIfAbsent(node, Destination::new).end();

        srvc.unregister(this);
    }

    /** */
    private final class Destination {
        /** */
        private final UUID nodeId;

        /** */
        private int hwm = -1;

        /** */
        private int lwm = -1;

        /** */
        private ArrayList<Object> curr = new ArrayList<>(ExchangeProcessor.BATCH_SIZE + 1); // extra space for end marker;

        /** */
        private boolean needSignal;

        /** */
        private Destination(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /**
         * Adds a row to current batch.
         *
         * @param row Row.
         */
        public void add(T row) {
            if (curr.size() == ExchangeProcessor.BATCH_SIZE) {
                assert ready() && srvc != null;

                srvc.send(queryId, exchangeId, nodeId, ++hwm, curr);

                curr = new ArrayList<>(ExchangeProcessor.BATCH_SIZE);
            }

            curr.add(row);
        }

        /**
         * Signals data is over.
         */
        public void end() {
            curr.add(EndMarker.INSTANCE);

            assert srvc != null;

            srvc.send(queryId, exchangeId, nodeId, hwm, curr);

            curr = null;
            hwm = Integer.MAX_VALUE;
        }

        /**
         * Checks whether there is a place for a new row.
         *
         * @return {@code True} is it possible to add a row to a batch.
         */
        boolean ready() {
            return hwm - lwm < ExchangeProcessor.PER_NODE_BATCH_COUNT || curr.size() < ExchangeProcessor.BATCH_SIZE;
        }

        /**
         * Callback method.
         *
         * @param id batch ID.
         */
        void onAcknowledge(int id) {
            if (lwm < id) {
                lwm = id;

                if (needSignal) {
                    needSignal = false;

                    signal();
                }
            }
        }

        /**
         * Sets "needSignal" flag.
         */
        public void needSignal() {
            needSignal = true;
        }
    }
}
