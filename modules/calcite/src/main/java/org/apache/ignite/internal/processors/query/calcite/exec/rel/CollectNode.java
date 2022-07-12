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
package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import com.google.common.collect.Iterables;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class CollectNode<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** */
    private final Collector<Row> collector;

    /** */
    private int requested;

    /** */
    private int waiting;

    /**
     * Creates Collect node with the collector depending on {@code rowType}.
     *
     * @param ctx Execution context.
     * @param rowType Output row type.
     */
    public CollectNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType
    ) {
        this(ctx, rowType, createCollector(ctx, rowType));
    }

    /**
     * Creates Collect node with the collector depending on {@code rowType}.
     *
     * @param ctx Execution context.
     * @param rowType Output row type.
     * @param collector Collector.
     */
    private CollectNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        Collector<Row> collector
    ) {
        super(ctx, rowType);

        this.collector = collector;
    }

    /**
     * Creates row counting Collect node.
     *
     * @param ctx Execution context.
     */
    public static <Row> CollectNode<Row> createCountCollector(ExecutionContext<Row> ctx) {
        RelDataType rowType = ctx.getTypeFactory().createSqlType(SqlTypeName.BIGINT);

        Collector<Row> collector = new Counter<>(
            ctx.rowHandler(),
            ctx.rowHandler().factory(ctx.getTypeFactory(), rowType),
            1);

        return new CollectNode<>(ctx, rowType, collector);
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        collector.clear();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;

        checkState();

        requested = rowsCnt;

        if (waiting == 0)
            source().request(waiting = IN_BUFFER_SIZE);
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting--;

        collector.push(row);

        if (waiting == 0)
            source().request(waiting = IN_BUFFER_SIZE);
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting = -1;

        if (isClosed())
            return;

        if (requested > 0) {
            requested = 0;

            downstream().push(collector.get());
            downstream().end();
        }
    }

    /** */
    private static <Row> Collector<Row> createCollector(
        ExecutionContext<Row> ctx,
        RelDataType rowType
    ) {
        IgniteTypeFactory typeFactory = ctx.getTypeFactory();
        RelDataType collectionType = Iterables.getOnlyElement(rowType.getFieldList()).getType();
        RowHandler.RowFactory<Row> rowFactory = ctx.rowHandler().factory(typeFactory, rowType);

        switch (collectionType.getSqlTypeName()) {
            case ARRAY:
                return new ArrayCollector<>(ctx.rowHandler(), rowFactory, IN_BUFFER_SIZE);
            case MAP:
                return new MapCollector<>(ctx.rowHandler(), rowFactory, IN_BUFFER_SIZE);
            default:
                throw new RuntimeException("Unsupported collectionType: " + collectionType.getSqlTypeName());
        }
    }

    /** */
    private abstract static class Collector<Row> implements Supplier<Row> {
        /** */
        protected final RowHandler<Row> rowHandler;

        /** */
        protected final RowHandler.RowFactory<Row> rowFactory;

        /** */
        protected final int cap;

        /** */
        Collector(
            RowHandler<Row> handler,
            RowHandler.RowFactory<Row> factory,
            int cap
        ) {
            rowHandler = handler;
            rowFactory = factory;
            this.cap = cap;
        }

        /** */
        public abstract void push(Row row);

        /** */
        public abstract void clear();

        /** */
        protected abstract Object outData();

        /** {@inheritDoc} */
        @Override public Row get() {
            Row out = rowFactory.create();

            rowHandler.set(0, out, outData());
            return out;
        }
    }

    /** */
    private static class MapCollector<Row> extends Collector<Row> {
        /** */
        private Map<Object, Object> outBuf;

        /** */
        private MapCollector(
            RowHandler<Row> handler,
            RowHandler.RowFactory<Row> rowFactory,
            int cap
        ) {
            super(handler, rowFactory, cap);
            outBuf = new LinkedHashMap<>(cap);
        }

        /** {@inheritDoc} */
        @Override protected Object outData() {
            return Collections.unmodifiableMap(outBuf);
        }

        /** {@inheritDoc} */
        @Override public void push(Row row) {
            outBuf.put(rowHandler.get(0, row), rowHandler.get(1, row));
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            outBuf = new LinkedHashMap<>(cap);
        }
    }

    /** */
    private static class ArrayCollector<Row> extends Collector<Row> {
        /** */
        private List<Object> outBuf;

        /** */
        private ArrayCollector(
            RowHandler<Row> handler,
            RowHandler.RowFactory<Row> rowFactory,
            int cap
        ) {
            super(handler, rowFactory, cap);
            outBuf = new ArrayList<>(cap);
        }

        /** {@inheritDoc} */
        @Override protected Object outData() {
            return Collections.unmodifiableList(outBuf);
        }

        /** {@inheritDoc} */
        @Override public void push(Row row) {
            outBuf.add(rowHandler.get(0, row));
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            outBuf = new ArrayList<>(cap);
        }
    }

    /** */
    private static class Counter<Row> extends Collector<Row> {
        /** */
        private long cnt;

        /** */
        private Counter(RowHandler<Row> hnd, RowHandler.RowFactory<Row> rowFactory, int cap) {
            super(hnd, rowFactory, cap);
        }

        /** {@inheritDoc} */
        @Override protected Object outData() {
            return cnt;
        }

        /** {@inheritDoc} */
        @Override public void push(Row row) {
            ++cnt;
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            cnt = 0;
        }
    }
}
