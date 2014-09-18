/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Task info.
 */
public class GridHadoopTaskInfo implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridHadoopTaskType type;

    /** */
    private GridHadoopJobId jobId;

    /** */
    private int taskNum;

    /** */
    private int attempt;

    /** */
    private GridHadoopInputSplit inputSplit;

    /**
     * For {@link Externalizable}.
     */
    public GridHadoopTaskInfo() {
        // No-op.
    }

    /**
     * Creates new task info.
     *
     * @param type Task type.
     * @param jobId Job id.
     * @param taskNum Task number.
     * @param attempt Attempt for this task.
     * @param inputSplit Input split.
     */
    public GridHadoopTaskInfo(GridHadoopTaskType type, GridHadoopJobId jobId, int taskNum, int attempt,
        @Nullable GridHadoopInputSplit inputSplit) {
        this.type = type;
        this.jobId = jobId;
        this.taskNum = taskNum;
        this.attempt = attempt;
        this.inputSplit = inputSplit;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(type.ordinal());
        out.writeObject(jobId);
        out.writeInt(taskNum);
        out.writeInt(attempt);
        out.writeObject(inputSplit);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type = GridHadoopTaskType.fromOrdinal(in.readByte());
        jobId = (GridHadoopJobId)in.readObject();
        taskNum = in.readInt();
        attempt = in.readInt();
        inputSplit = (GridHadoopInputSplit)in.readObject();
    }

    /**
     * @return Type.
     */
    public GridHadoopTaskType type() {
        return type;
    }

    /**
     * @return Job id.
     */
    public GridHadoopJobId jobId() {
        return jobId;
    }

    /**
     * @return Task number.
     */
    public int taskNumber() {
        return taskNum;
    }

    /**
     * @return Attempt.
     */
    public int attempt() {
        return attempt;
    }

    /**
     * @return Input split.
     */
    @Nullable public GridHadoopInputSplit inputSplit() {
        return inputSplit;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof GridHadoopTaskInfo))
            return false;

        GridHadoopTaskInfo that = (GridHadoopTaskInfo)o;

        return attempt == that.attempt && taskNum == that.taskNum && jobId.equals(that.jobId) && type == that.type;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = type.hashCode();

        res = 31 * res + jobId.hashCode();
        res = 31 * res + taskNum;
        res = 31 * res + attempt;

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopTaskInfo.class, this);
    }
}
