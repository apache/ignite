/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer.task;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.streamer.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.closure.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Streamer broadcast task.
 */
public class GridStreamerBroadcastTask extends GridPeerDeployAwareTaskAdapter<Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Closure. */
    private IgniteInClosure<StreamerContext> clo;

    /** Streamer. */
    private String streamer;

    /**
     * @param clo Closure.
     * @param streamer Streamer.
     */
    public GridStreamerBroadcastTask(IgniteInClosure<StreamerContext> clo, @Nullable String streamer) {
        super(U.peerDeployAware(clo));

        this.clo = clo;
        this.streamer = streamer;
    }

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Void arg)
        throws GridException {
        Map<ComputeJob, ClusterNode> res = U.newHashMap(subgrid.size());

        for (ClusterNode node : subgrid)
            res.put(new StreamerBroadcastJob(clo, streamer), node);

        return res;
    }

    /** {@inheritDoc} */
    @Override public Void reduce(List<ComputeJobResult> results) throws GridException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws GridException {
        // No failover.
        if (res.getException() != null)
            throw res.getException();

        return ComputeJobResultPolicy.WAIT;
    }

    /**
     * Streamer broadcast job.
     */
    private static class StreamerBroadcastJob extends ComputeJobAdapter implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Injected grid. */
        @IgniteInstanceResource
        private Ignite g;

        /** Closure. */
        private IgniteInClosure<StreamerContext> clo;

        /** Streamer. */
        private String streamer;

        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public StreamerBroadcastJob() {
            // No-op.
        }

        /**
         * @param clo Closure.
         * @param streamer Streamer.
         */
        private StreamerBroadcastJob(IgniteInClosure<StreamerContext> clo, String streamer) {
            this.clo = clo;
            this.streamer = streamer;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws GridException {
            IgniteStreamer s = g.streamer(streamer);

            assert s != null;

            clo.apply(s.context());

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(clo);
            U.writeString(out, streamer);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            clo = (IgniteInClosure<StreamerContext>)in.readObject();
            streamer = U.readString(in);
        }
    }
}
