/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.streamer;

import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for {@link GridStreamerMetrics}.
 */
public class VisorStreamerMetrics implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Active stages. */
    private int active;

    /** Waiting stages. */
    private int waiting;

    /** Stages execution capacity. */
    private int capacity;

    /** Pipeline minimum execution time. */
    private long pipelineMinExecTm;

    /** Pipeline average execution time. */
    private long pipelineAvgExecTm;

    /** Pipeline maximum execution time. */
    private long pipelineMaxExecTm;

    /** Minimum number of unique nodes in pipeline execution. */
    private int pipelineMinExecNodes;

    /** Average number of unique nodes in pipeline execution. */
    private int pipelineAvgExecNodes;

    /** Maximum number of unique nodes in pipeline execution. */
    private int pipelineMaxExecNodes;

    /** Query minimum execution time. */
    private long qryMinExecTm;

    /** Query average execution time. */
    private long qryAvgExecTm;

    /** Query maximum execution time. */
    private long qryMaxExecTm;

    /** Minimum number of unique nodes in query execution. */
    private int qryMinExecNodes;

    /** Average number of unique nodes in query execution. */
    private int qryAvgExecNodes;

    /** Maximum number of unique nodes in query execution. */
    private int qryMaxExecNodes;

    /** Current window size. */
    private int windowSize;

    /**
     * @param streamer Source streamer.
     * @return Data transfer streamer for given streamer.
     */
    public static VisorStreamerMetrics from(GridStreamer streamer) {
        assert streamer != null;

        GridStreamerMetrics m = streamer.metrics();

        int windowSz = 0;

        for (GridStreamerWindowMetrics wm : m.windowMetrics())
            windowSz += wm.size();

        VisorStreamerMetrics metrics = new VisorStreamerMetrics();

        metrics.active(m.stageActiveExecutionCount());
        metrics.waiting(m.stageWaitingExecutionCount());
        metrics.capacity(m.executorServiceCapacity());
        
        metrics.pipelineMinExecutionTime(m.pipelineMinimumExecutionTime());
        metrics.pipelineAvgExecutionTime(m.pipelineAverageExecutionTime());
        metrics.pipelineMaxExecutionTime(m.pipelineMaximumExecutionTime());

        metrics.pipelineMinExecutionNodes(m.pipelineMinimumExecutionNodes());
        metrics.pipelineAvgExecutionNodes(m.pipelineAverageExecutionNodes());
        metrics.pipelineMaxExecutionNodes(m.pipelineMaximumExecutionNodes());

        metrics.queryMinExecutionTime(m.queryMinimumExecutionTime());
        metrics.queryAvgExecutionTime(m.queryAverageExecutionTime());
        metrics.queryMaxExecutionTime(m.queryMaximumExecutionTime());

        metrics.queryMinExecutionNodes(m.queryMinimumExecutionNodes());
        metrics.queryAvgExecutionNodes(m.queryAverageExecutionNodes());
        metrics.queryMaxExecutionNodes(m.queryMaximumExecutionNodes());

        metrics.windowSize(windowSz);

        return metrics;
    }

    /**
     * @return Active stages.
     */
    public int active() {
        return active;
    }

    /**
     * @param active New active stages.
     */
    public void active(int active) {
        this.active = active;
    }

    /**
     * @return Waiting stages.
     */
    public int waiting() {
        return waiting;
    }

    /**
     * @param waiting New waiting stages.
     */
    public void waiting(int waiting) {
        this.waiting = waiting;
    }

    /**
     * @return Stages execution capacity.
     */
    public int capacity() {
        return capacity;
    }

    /**
     * @param cap New stages execution capacity.
     */
    public void capacity(int cap) {
        capacity = cap;
    }

    /**
     * @return Pipeline minimum execution time.
     */
    public long pipelineMinExecutionTime() {
        return pipelineMinExecTm;
    }

    /**
     * @param pipelineMinExecTm New pipeline minimum execution time.
     */
    public void pipelineMinExecutionTime(long pipelineMinExecTm) {
        this.pipelineMinExecTm = pipelineMinExecTm;
    }

    /**
     * @return Pipeline average execution time.
     */
    public long pipelineAvgExecutionTime() {
        return pipelineAvgExecTm;
    }

    /**
     * @param pipelineAvgExecTm New pipeline average execution time.
     */
    public void pipelineAvgExecutionTime(long pipelineAvgExecTm) {
        this.pipelineAvgExecTm = pipelineAvgExecTm;
    }

    /**
     * @return Pipeline maximum execution time.
     */
    public long pipelineMaxExecutionTime() {
        return pipelineMaxExecTm;
    }

    /**
     * @param pipelineMaxExecTm New pipeline maximum execution time.
     */
    public void pipelineMaxExecutionTime(long pipelineMaxExecTm) {
        this.pipelineMaxExecTm = pipelineMaxExecTm;
    }

    /**
     * @return Minimum number of unique nodes in pipeline execution.
     */
    public int pipelineMinExecutionNodes() {
        return pipelineMinExecNodes;
    }

    /**
     * @param pipelineMinExecNodes New minimum number of unique nodes in pipeline execution.
     */
    public void pipelineMinExecutionNodes(int pipelineMinExecNodes) {
        this.pipelineMinExecNodes = pipelineMinExecNodes;
    }

    /**
     * @return Average number of unique nodes in pipeline execution.
     */
    public int pipelineAvgExecutionNodes() {
        return pipelineAvgExecNodes;
    }

    /**
     * @param pipelineAvgExecNodes New average number of unique nodes in pipeline execution.
     */
    public void pipelineAvgExecutionNodes(int pipelineAvgExecNodes) {
        this.pipelineAvgExecNodes = pipelineAvgExecNodes;
    }

    /**
     * @return Maximum number of unique nodes in pipeline execution.
     */
    public int pipelineMaxExecutionNodes() {
        return pipelineMaxExecNodes;
    }

    /**
     * @param pipelineMaxExecNodes New maximum number of unique nodes in pipeline execution.
     */
    public void pipelineMaxExecutionNodes(int pipelineMaxExecNodes) {
        this.pipelineMaxExecNodes = pipelineMaxExecNodes;
    }

    /**
     * @return Query minimum execution time.
     */
    public long queryMinExecutionTime() {
        return qryMinExecTm;
    }

    /**
     * @param qryMinExecTime New query minimum execution time.
     */
    public void queryMinExecutionTime(long qryMinExecTime) {
        qryMinExecTm = qryMinExecTime;
    }

    /**
     * @return Query average execution time.
     */
    public long queryAvgExecutionTime() {
        return qryAvgExecTm;
    }

    /**
     * @param qryAvgExecTime New query average execution time.
     */
    public void queryAvgExecutionTime(long qryAvgExecTime) {
        qryAvgExecTm = qryAvgExecTime;
    }

    /**
     * @return Query maximum execution time.
     */
    public long queryMaxExecutionTime() {
        return qryMaxExecTm;
    }

    /**
     * @param qryMaxExecTime New query maximum execution time.
     */
    public void queryMaxExecutionTime(long qryMaxExecTime) {
        qryMaxExecTm = qryMaxExecTime;
    }

    /**
     * @return Minimum number of unique nodes in query execution.
     */
    public int queryMinExecutionNodes() {
        return qryMinExecNodes;
    }

    /**
     * @param qryMinExecNodes New minimum number of unique nodes in query execution.
     */
    public void queryMinExecutionNodes(int qryMinExecNodes) {
        this.qryMinExecNodes = qryMinExecNodes;
    }

    /**
     * @return Average number of unique nodes in query execution.
     */
    public int queryAvgExecutionNodes() {
        return qryAvgExecNodes;
    }

    /**
     * @param qryAvgExecNodes New average number of unique nodes in query execution.
     */
    public void queryAvgExecutionNodes(int qryAvgExecNodes) {
        this.qryAvgExecNodes = qryAvgExecNodes;
    }

    /**
     * @return Maximum number of unique nodes in query execution.
     */
    public int queryMaxExecutionNodes() {
        return qryMaxExecNodes;
    }

    /**
     * @param qryMaxExecNodes New maximum number of unique nodes in query execution.
     */
    public void queryMaxExecutionNodes(int qryMaxExecNodes) {
        this.qryMaxExecNodes = qryMaxExecNodes;
    }

    /**
     * @return Current window size.
     */
    public int windowSize() {
        return windowSize;
    }

    /**
     * @param windowSize New current window size.
     */
    public void windowSize(int windowSize) {
        this.windowSize = windowSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorStreamerMetrics.class, this);
    }
}
