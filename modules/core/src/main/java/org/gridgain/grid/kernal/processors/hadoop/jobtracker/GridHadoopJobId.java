/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.jobtracker;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Job ID.
 */
public class GridHadoopJobId implements Externalizable {
    /** */
    private UUID nodeId;

    /** */
    private int jobId;

    /**
     * @param nodeId Node ID.
     * @param jobId Job ID.
     */
    public GridHadoopJobId(UUID nodeId, int jobId) {
        this.nodeId = nodeId;
        this.jobId = jobId;
    }

    public UUID globalId() {
        return nodeId;
    }

    public int localId() {
        return jobId;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeUuid(out, nodeId);
        out.writeInt(jobId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = U.readUuid(in);
        jobId = in.readInt();
    }
}
