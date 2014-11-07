/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Hadoop job status.
 */
public class GridHadoopJobStatus implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Job ID. */
    private GridHadoopJobId jobId;

    /** Job name. */
    private String jobName;

    /** User. */
    private String usr;

    /** Pending mappers count. */
    private int pendingMapperCnt;

    /** Pending reducers count. */
    private int pendingReducerCnt;

    /** Total mappers count. */
    private int totalMapperCnt;

    /** Total reducers count. */
    private int totalReducerCnt;
    /** Phase. */
    private GridHadoopJobPhase jobPhase;

    /** */
    private boolean failed;

    /** Version. */
    private long ver;

    /**
     * {@link Externalizable} support.
     */
    public GridHadoopJobStatus() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param jobId Job ID.
     * @param jobName Job name.
     * @param usr User.
     * @param pendingMapperCnt Pending mappers count.
     * @param pendingReducerCnt Pending reducers count.
     * @param totalMapperCnt Total mappers count.
     * @param totalReducerCnt Total reducers count.
     * @param jobPhase Job phase.
     * @param failed Failed.
     * @param ver Version.
     */
    public GridHadoopJobStatus(
        GridHadoopJobId jobId,
        String jobName,
        String usr,
        int pendingMapperCnt,
        int pendingReducerCnt,
        int totalMapperCnt,
        int totalReducerCnt,
        GridHadoopJobPhase jobPhase,
        boolean failed,
        long ver
    ) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.usr = usr;
        this.pendingMapperCnt = pendingMapperCnt;
        this.pendingReducerCnt = pendingReducerCnt;
        this.totalMapperCnt = totalMapperCnt;
        this.totalReducerCnt = totalReducerCnt;
        this.jobPhase = jobPhase;
        this.failed = failed;
        this.ver = ver;
    }

    /**
     * @return Job ID.
     */
    public GridHadoopJobId jobId() {
        return jobId;
    }

    /**
     * @return Job name.
     */
    public String jobName() {
        return jobName;
    }

    /**
     * @return User.
     */
    public String user() {
        return usr;
    }

    /**
     * @return Pending mappers count.
     */
    public int pendingMapperCnt() {
        return pendingMapperCnt;
    }

    /**
     * @return Pending reducers count.
     */
    public int pendingReducerCnt() {
        return pendingReducerCnt;
    }

    /**
     * @return Total mappers count.
     */
    public int totalMapperCnt() {
        return totalMapperCnt;
    }

    /**
     * @return Total reducers count.
     */
    public int totalReducerCnt() {
        return totalReducerCnt;
    }

    /**
     * @return Version.
     */
    public long version() {
        return ver;
    }

    /**
     * @return Job phase.
     */
    public GridHadoopJobPhase jobPhase() {
        return jobPhase;
    }

    /**
     * @return {@code true} If the job failed.
     */
    public boolean isFailed() {
        return failed;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopJobStatus.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(jobId);
        U.writeString(out, jobName);
        U.writeString(out, usr);
        out.writeInt(pendingMapperCnt);
        out.writeInt(pendingReducerCnt);
        out.writeInt(totalMapperCnt);
        out.writeInt(totalReducerCnt);
        out.writeObject(jobPhase);
        out.writeBoolean(failed);
        out.writeLong(ver);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = (GridHadoopJobId)in.readObject();
        jobName = U.readString(in);
        usr = U.readString(in);
        pendingMapperCnt = in.readInt();
        pendingReducerCnt = in.readInt();
        totalMapperCnt = in.readInt();
        totalReducerCnt = in.readInt();
        jobPhase = (GridHadoopJobPhase)in.readObject();
        failed = in.readBoolean();
        ver = in.readLong();
    }
}
