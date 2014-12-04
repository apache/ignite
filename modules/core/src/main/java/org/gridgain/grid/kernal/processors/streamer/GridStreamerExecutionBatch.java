/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer;

import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Streamer execution batch which should be processed on one node.
 */
public class GridStreamerExecutionBatch implements Externalizable, IgniteOptimizedMarshallable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "AbbreviationUsage", "UnusedDeclaration"})
    private static Object GG_CLASS_ID;

    /** Execution ID (ID of root future). */
    private IgniteUuid execId;

    /** Execution start timestamp. */
    private long execStartTs;

    /** Originating future ID. */
    private IgniteUuid futId;

    /** Nodes participated in this execution. */
    @GridToStringInclude
    private Collection<UUID> execNodeIds;

    /** Stage name. */
    private String stageName;

    /** Events to process. */
    private Collection<Object> evts;

    /** Deployment. This is transient field. */
    private transient GridDeployment dep;

    /**
     * Empty constructor required by {@code Externalizable}.
     */
    public GridStreamerExecutionBatch() {
        // No-op.
    }

    /**
     * Execution batch.
     *
     * @param execId Execution ID.
     * @param futId Future ID.
     * @param execStartTs Execution start timestamp.
     * @param execNodeIds Nodes participated in this execution.
     * @param stageName Stage name to execute.
     * @param evts Events to process.
     */
    public GridStreamerExecutionBatch(
        IgniteUuid execId,
        long execStartTs,
        IgniteUuid futId,
        Collection<UUID> execNodeIds,
        String stageName,
        Collection<Object> evts
    ) {
        assert stageName != null;

        this.execId = execId;
        this.futId = futId;
        this.execStartTs = execStartTs;
        this.execNodeIds = execNodeIds;
        this.stageName = stageName;
        this.evts = evts;
    }

    /**
     * Sets deployment. Deployment is not marshalled.
     *
     * @param dep Deployment for batch.
     */
    public void deployment(GridDeployment dep) {
        this.dep = dep;
    }

    /**
     * @return Batch deployment, if any.
     */
    @Nullable GridDeployment deployment() {
        return dep;
    }

    /**
     * @return Execution ID.
     */
    public IgniteUuid executionId() {
        return execId;
    }

    /**
     * @return Execution start timestamp.
     */
    public long executionStartTimeStamp() {
        return execStartTs;
    }

    /**
     * @return Batch future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Execution node IDs.
     */
    public Collection<UUID> executionNodeIds() {
        return execNodeIds;
    }

    /**
     * @return Stage name.
     */
    public String stageName() {
        return stageName;
    }

    /**
     * @return Events collection.
     */
    public Collection<Object> events() {
        return evts;
    }

    /** {@inheritDoc} */
    @Override public Object ggClassId() {
        return GG_CLASS_ID;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, execId);
        out.writeLong(execStartTs);
        U.writeGridUuid(out, futId);
        U.writeUuids(out, execNodeIds);
        out.writeUTF(stageName);
        U.writeCollection(out, evts);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        execId = U.readGridUuid(in);
        execStartTs = in.readLong();
        futId = U.readGridUuid(in);
        execNodeIds = U.readUuids(in);
        stageName = in.readUTF();
        evts = U.readCollection(in);
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridStreamerExecutionBatch.class, this);
    }
}
