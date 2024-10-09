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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProxyImpl;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.ModifyTuple;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.CONCURRENT_UPDATE;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.DUPLICATE_KEY;

/**
 *
 */
public class ModifyNode<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** */
    protected final CacheTableDescriptor desc;

    /** */
    private final TableModify.Operation op;

    /** */
    private final List<String> cols;

    /** */
    private List<ModifyTuple> tuples = new ArrayList<>(MODIFY_BATCH_SIZE);

    /** */
    private long updatedRows;

    /** */
    private int waiting;

    /** */
    private int requested;

    /** */
    private boolean inLoop;

    /** */
    private State state = State.UPDATING;

    /**
     * @param ctx Execution context.
     * @param desc Table descriptor.
     * @param cols Update column list.
     */
    public ModifyNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        CacheTableDescriptor desc,
        TableModify.Operation op,
        List<String> cols
    ) {
        super(ctx, rowType);

        this.desc = desc;
        this.op = op;
        this.cols = cols;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;

        checkState();

        requested = rowsCnt;

        if (!inLoop)
            tryEnd();
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;
        assert state == State.UPDATING;

        checkState();

        waiting--;

        switch (op) {
            case DELETE:
            case UPDATE:
            case INSERT:
            case MERGE:
                tuples.add(desc.toTuple(context(), row, op, cols));

                flushTuples(false);

                break;
            default:
                throw new UnsupportedOperationException(op.name());
        }

        if (waiting == 0)
            source().request(waiting = MODIFY_BATCH_SIZE);
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting = -1;
        state = State.UPDATED;

        tryEnd();
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** */
    private void tryEnd() throws Exception {
        assert downstream() != null;

        if (state == State.UPDATING && waiting == 0)
            source().request(waiting = MODIFY_BATCH_SIZE);

        if (state == State.UPDATED && requested > 0) {
            flushTuples(true);

            state = State.END;

            inLoop = true;
            try {
                requested--;
                downstream().push(context().rowHandler().factory(long.class).create(updatedRows));
            }
            finally {
                inLoop = false;
            }
        }

        if (state == State.END && requested > 0) {
            requested = 0;
            downstream().end();
        }
    }

    /** */
    @SuppressWarnings("unchecked")
    private void flushTuples(boolean force) throws IgniteCheckedException {
        if (F.isEmpty(tuples) || !force && tuples.size() < MODIFY_BATCH_SIZE)
            return;

        List<ModifyTuple> tuples = this.tuples;
        this.tuples = new ArrayList<>(MODIFY_BATCH_SIZE);

        GridCacheContext<Object, Object> cctx = desc.cacheContext();
        GridCacheProxyImpl<Object, Object> cache = cctx.cache().keepBinary();
        GridNearTxLocal tx = Commons.queryTransaction(context(), cctx.shared());

        if (tx == null)
            invokeOutsideTransaction(tuples, cache);
        else
            invokeInsideTransaction(tuples, cache, tx);
    }

    /**
     * Perform data modification without explicit transaction.
     * @param tuples Modified data.
     * @param cache Cache.
     * @throws IgniteCheckedException If failed.
     */
    private void invokeOutsideTransaction(
        List<ModifyTuple> tuples,
        GridCacheProxyImpl<Object, Object> cache
    ) throws IgniteCheckedException {
        Map<Object, EntryProcessor<Object, Object, Long>> map = invokeMap(tuples);
        Map<Object, EntryProcessorResult<Long>> res = cache.invokeAll(map);

        long updated = res.values().stream().mapToLong(EntryProcessorResult::get).sum();

        if ((op == TableModify.Operation.INSERT || op == TableModify.Operation.MERGE) && updated != res.size()) {
            List<Object> conflictKeys = res.entrySet().stream()
                .filter(e -> e.getValue().get() == 0)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

            throw conflictKeysException(conflictKeys);
        }

        updatedRows += updated;
    }

    /**
     * Performs data modification within user transaction.
     * @param tuples Modified data.
     * @param cache Cache.
     * @param userTx Transaction.
     * @throws IgniteCheckedException If failed.
     */
    private void invokeInsideTransaction(
        List<ModifyTuple> tuples,
        GridCacheProxyImpl<Object, Object> cache,
        GridNearTxLocal userTx
    ) throws IgniteCheckedException {
        userTx.resume();

        try {
            invokeOutsideTransaction(tuples, cache);
        }
        finally {
            userTx.suspend();
        }
    }

    /** */
    private IgniteSQLException conflictKeysException(List<Object> conflictKeys) {
        if (op == TableModify.Operation.INSERT) {
            return new IgniteSQLException("Failed to INSERT some keys because they are already in cache. " +
                "[cache=" + desc.cacheContext().name() + ", keys=" + conflictKeys + ']', DUPLICATE_KEY);
        }
        else {
            return new IgniteSQLException("Failed to MERGE some keys due to keys conflict or concurrent updates. " +
                "[cache=" + desc.cacheContext().name() + ", keys=" + conflictKeys + ']', CONCURRENT_UPDATE);
        }
    }

    /** */
    private Map<Object, EntryProcessor<Object, Object, Long>> invokeMap(List<ModifyTuple> tuples) {
        Map<Object, EntryProcessor<Object, Object, Long>> procMap = U.newLinkedHashMap(tuples.size());

        for (ModifyTuple entry : tuples) {
            assert entry.getOp() == op || op == TableModify.Operation.MERGE : entry.getOp();

            switch (entry.getOp()) {
                case INSERT:
                    if (procMap.put(entry.getKey(), new InsertOperation<>(entry.getValue())) != null)
                        throw conflictKeysException(Collections.singletonList(entry.getKey()));

                    break;
                case UPDATE:
                    procMap.put(entry.getKey(), new UpdateOperation<>(entry.getValue()));

                    break;
                case DELETE:
                    assert op == TableModify.Operation.DELETE;

                    procMap.put(entry.getKey(), new DeleteOperation<>());

                    break;
                default:
                    throw new AssertionError("Unexpected tuple operation: " + entry.getOp());
            }
        }

        return procMap;
    }

    /** */
    private enum State {
        /** */
        UPDATING,

        /** */
        UPDATED,

        /** */
        END
    }

    /** */
    private static class InsertOperation<K, V> implements EntryProcessor<K, V, Long> {
        /** */
        private final V val;

        /** */
        private InsertOperation(V val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<K, V> entry, Object... arguments) throws EntryProcessorException {
            if (!entry.exists()) {
                entry.setValue(val);

                return 1L;
            }

            return 0L;
        }
    }

    /** */
    private static class UpdateOperation<K, V> implements EntryProcessor<K, V, Long> {
        /** */
        private final V val;

        /** */
        private UpdateOperation(V val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<K, V> entry, Object... arguments) throws EntryProcessorException {
            if (entry.exists()) {
                entry.setValue(val);

                return 1L;
            }

            return 0L;
        }
    }

    /** */
    private static class DeleteOperation<K, V> implements EntryProcessor<K, V, Long> {
        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<K, V> entry, Object... arguments) throws EntryProcessorException {
            if (entry.exists()) {
                entry.remove();

                return 1L;
            }

            return 0L;
        }
    }
}
