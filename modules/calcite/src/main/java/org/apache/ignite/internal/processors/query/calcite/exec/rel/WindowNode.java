package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.window.WindowPartition;
import org.apache.ignite.internal.util.typedef.F;

/** Window support node */
public class WindowNode<Row> extends MemoryTrackingNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /**  */
    private final Comparator<Row> partCmp;

    /**  */
    private final Supplier<WindowPartition<Row>> partitionFactory;

    /**  */
    private final RowHandler.RowFactory<Row> rowFactory;

    /**  */
    private WindowPartition<Row> partition;

    /**  */
    private int requested;

    /**  */
    private int waiting;

    /**  */
    private Row prevRow;

    /**  */
    private final Deque<Row> outBuf = new ArrayDeque<>(IN_BUFFER_SIZE);

    /**  */
    public WindowNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        Comparator<Row> partCmp,
        Supplier<WindowPartition<Row>> partitionFactory,
        RowHandler.RowFactory<Row> rowFactory
    ) {
        super(ctx, rowType, DFLT_ROW_OVERHEAD);
        this.partCmp = partCmp;
        this.partitionFactory = partitionFactory;
        this.rowFactory = rowFactory;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0;

        checkState();

        requested = rowsCnt;

        doPush();

        if (waiting == 0) {
            waiting = IN_BUFFER_SIZE;
            source().request(IN_BUFFER_SIZE);
        }
        else if (waiting < 0)
            downstream().end();
    }

    @Override public void push(Row row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting--;

        if (partition == null) {
            partition = partitionFactory.get();
        }
        else if (prevRow != null && partCmp != null && partCmp.compare(prevRow, row) != 0) {
            partition.drainTo(rowFactory, outBuf);
            partition.reset();
            doPush();
        }

        if (partition.add(row)) {
            partition.drainTo(rowFactory, outBuf);
            doPush();
        }
        else
            nodeMemoryTracker.onRowAdded(row);

        prevRow = row;

        if (waiting == 0 && requested > 0) {
            waiting = IN_BUFFER_SIZE;

            context().execute(() -> source().request(IN_BUFFER_SIZE), this::onError);
        }
    }

    @Override public void end() throws Exception {
        assert downstream() != null;
        if (waiting < 0) {
            return;
        }

        waiting = -1;

        checkState();

        if (partition != null) {
            partition.drainTo(rowFactory, outBuf);
            partition.reset();
        }

        doPush();

        downstream().end();
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        if (partition != null) {
            partition.reset();
            partition = null;
        }
        outBuf.clear();
        nodeMemoryTracker.reset();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /**  */
    private void doPush() throws Exception {
        while (requested > 0 && !outBuf.isEmpty()) {
            requested--;

            downstream().push(outBuf.poll());
        }
    }
}
