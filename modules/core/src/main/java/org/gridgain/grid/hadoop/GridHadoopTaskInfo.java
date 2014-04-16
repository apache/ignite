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
import java.util.*;

/**
 * Task info.
 */
public class GridHadoopTaskInfo implements Externalizable {
    /** */
    private UUID nodeId;

    /** */
    private GridHadoopTaskType type;

    /** */
    private GridHadoopJobId jobId;

    /** */
    private int taskNumber;

    /** */
    private int attempt;

    /** */
    private GridHadoopFileBlock fileBlock;

    /**
     * For {@link Externalizable}.
     */
    public GridHadoopTaskInfo() {
        // No-op.
    }

    /**
     * Creates new task info.
     *
     * @param nodeId Node id.
     * @param type Task type.
     * @param jobId Job id.
     * @param taskNumber Task number.
     * @param attempt Attempt for this task.
     * @param fileBlock File block.
     */
    public GridHadoopTaskInfo(UUID nodeId, GridHadoopTaskType type, GridHadoopJobId jobId, int taskNumber, int attempt,
        @Nullable GridHadoopFileBlock fileBlock) {
        this.nodeId = nodeId;
        this.type = type;
        this.jobId = jobId;
        this.taskNumber = taskNumber;
        this.attempt = attempt;
        this.fileBlock = fileBlock;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(type.ordinal());
        U.writeUuid(out, nodeId);
        out.writeObject(jobId);
        out.writeInt(taskNumber);
        out.writeInt(attempt);
        out.writeObject(fileBlock);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type = GridHadoopTaskType.fromOrdinal(in.readByte());
        nodeId = U.readUuid(in);
        jobId = (GridHadoopJobId)in.readObject();
        taskNumber = in.readInt();
        attempt = in.readInt();
        fileBlock = (GridHadoopFileBlock)in.readObject();
    }

    /**
     * @return Node id.
     */
    public UUID nodeId() {
        return nodeId;
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
        return taskNumber;
    }

    /**
     * @return Attempt.
     */
    public int attempt() {
        return attempt;
    }

    /**
     * @return File block.
     */
    @Nullable public GridHadoopFileBlock fileBlock() {
        return fileBlock;
    }
}
