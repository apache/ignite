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

    /** Job state. */
    private GridHadoopJobState jobState;

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

    /** Setup start timestamp. */
    private long setupStartTs;

    /** Map start time. */
    private long mapStartTs;

    /** Reduce start time. */
    private long reduceStartTs;

    /** Phase. */
    private GridHadoopJobPhase jobPhase;

    /** Speculative concurrency level. */
    private int concurrencyLvl;

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
     * @param jobState Job state.
     * @param jobName Job name.
     * @param usr User.
     * @param pendingMapperCnt Pending mappers count.
     * @param pendingReducerCnt Pending reducers count.
     * @param totalMapperCnt Total mappers count.
     * @param totalReducerCnt Total reducers count.
     * @param setupStartTs Map start time.
     * @param mapStartTs Map start time.
     * @param reduceStartTs Reduce start time.
     * @param jobPhase Job phase.
     * @param concurrencyLvl Speculative concurrency level.
     * @param ver Version.
     */
    public GridHadoopJobStatus(
        GridHadoopJobId jobId,
        GridHadoopJobState jobState,
        String jobName,
        String usr,
        int pendingMapperCnt,
        int pendingReducerCnt,
        int totalMapperCnt,
        int totalReducerCnt,
        long setupStartTs,
        long mapStartTs,
        long reduceStartTs,
        GridHadoopJobPhase jobPhase,
        int concurrencyLvl,
        long ver
    ) {
        this.jobId = jobId;
        this.jobState = jobState;
        this.jobName = jobName;
        this.usr = usr;
        this.pendingMapperCnt = pendingMapperCnt;
        this.pendingReducerCnt = pendingReducerCnt;
        this.totalMapperCnt = totalMapperCnt;
        this.totalReducerCnt = totalReducerCnt;
        this.setupStartTs = setupStartTs;
        this.mapStartTs = mapStartTs;
        this.reduceStartTs = reduceStartTs;
        this.jobPhase = jobPhase;
        this.concurrencyLvl = concurrencyLvl;
        this.ver = ver;
    }

    /**
     * @return Job ID.
     */
    public GridHadoopJobId jobId() {
        return jobId;
    }

    /**
     * @return Job state.
     */
    public GridHadoopJobState jobState() {
        return jobState;
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
     * @return Setup start time.
     */
    public long setupStartTime() {
        return setupStartTs;
    }

    /**
     * @return Map start time.
     */
    public long mapStartTime() {
        return mapStartTs;
    }

    /**
     * @return Reduce start time.
     */
    public long reduceStartTime() {
        return reduceStartTs;
    }

    /**
     * @return Speculative concurrency level.
     */
    public int concurrencyLevel() {
        return concurrencyLvl;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopJobStatus.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(jobId);
        out.writeObject(jobState);
        U.writeString(out, jobName);
        U.writeString(out, usr);
        out.writeInt(pendingMapperCnt);
        out.writeInt(pendingReducerCnt);
        out.writeInt(totalMapperCnt);
        out.writeInt(totalReducerCnt);
        out.writeLong(setupStartTs);
        out.writeLong(mapStartTs);
        out.writeLong(reduceStartTs);
        out.writeObject(jobPhase);
        out.writeInt(concurrencyLvl);
        out.writeLong(ver);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = (GridHadoopJobId)in.readObject();
        jobState = (GridHadoopJobState)in.readObject();
        jobName = U.readString(in);
        usr = U.readString(in);
        pendingMapperCnt = in.readInt();
        pendingReducerCnt = in.readInt();
        totalMapperCnt = in.readInt();
        totalReducerCnt = in.readInt();
        setupStartTs = in.readLong();
        mapStartTs = in.readLong();
        reduceStartTs = in.readLong();
        jobPhase = (GridHadoopJobPhase)in.readObject();
        concurrencyLvl = in.readInt();
        ver = in.readLong();
    }
}
