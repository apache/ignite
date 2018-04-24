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
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.processors.cache.verify.PartitionEntryHashRecord;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Result for task {@link VisorIdleAnalyzeTask}
 */
public class VisorIdleAnalyzeTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Results. */
    private Map<PartitionHashRecord, List<PartitionEntryHashRecord>> divergedEntries;

    /**
     * Default constructor.
     */
    public VisorIdleAnalyzeTaskResult() {
        // No-op.
    }

    /**
     * @param divergedEntries Result.
     */
    public VisorIdleAnalyzeTaskResult(Map<PartitionHashRecord, List<PartitionEntryHashRecord>> divergedEntries) {
        this.divergedEntries = divergedEntries;
    }

    /**
     * @return Results.
     */
    public Map<PartitionHashRecord, List<PartitionEntryHashRecord>> getDivergedEntries() {
        return divergedEntries;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, divergedEntries);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        divergedEntries = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorIdleAnalyzeTaskResult.class, this);
    }
}
