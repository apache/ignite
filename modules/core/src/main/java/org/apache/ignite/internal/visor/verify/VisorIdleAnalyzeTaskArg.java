/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.visor.verify;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.processors.cache.verify.PartitionKey;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Arguments for task {@link VisorIdleAnalyzeTask}
 */
public class VisorIdleAnalyzeTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Partition key. */
    private PartitionKey partKey;

    /**
     * Default constructor.
     */
    public VisorIdleAnalyzeTaskArg() {
        // No-op.
    }

    /**
     * @param partKey Partition key.
     */
    public VisorIdleAnalyzeTaskArg(PartitionKey partKey) {
        this.partKey = partKey;
    }

    /**
     * @param grpId Group id.
     * @param partId Partition id.
     * @param grpName Group name.
     */
    public VisorIdleAnalyzeTaskArg(int grpId, int partId, String grpName) {
        this(new PartitionKey(grpId, partId, grpName));
    }

    /**
     * @return Partition key.
     */
    public PartitionKey getPartitionKey() {
        return partKey;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(partKey.groupId());
        out.writeInt(partKey.partitionId());
        U.writeString(out, partKey.groupName());
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException {
        int grpId = in.readInt();
        int partId = in.readInt();
        String grpName = U.readString(in);

        partKey = new PartitionKey(grpId, partId, grpName);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorIdleAnalyzeTaskArg.class, this);
    }
}
