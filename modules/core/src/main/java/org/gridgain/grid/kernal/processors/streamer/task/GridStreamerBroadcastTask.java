/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer.task;

import org.gridgain.grid.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.closure.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.streamer.*;
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
    private GridInClosure<GridStreamerContext> clo;

    /** Streamer. */
    private String streamer;

    /**
     * @param clo Closure.
     * @param streamer Streamer.
     */
    public GridStreamerBroadcastTask(GridInClosure<GridStreamerContext> clo, @Nullable String streamer) {
        super(U.peerDeployAware(clo));

        this.clo = clo;
        this.streamer = streamer;
    }

    /** {@inheritDoc} */
    @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, @Nullable Void arg)
        throws GridException {
        Map<GridComputeJob, GridNode> res = U.newHashMap(subgrid.size());

        for (GridNode node : subgrid)
            res.put(new StreamerBroadcastJob(clo, streamer), node);

        return res;
    }

    /** {@inheritDoc} */
    @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd) throws GridException {
        // No failover.
        if (res.getException() != null)
            throw res.getException();

        return GridComputeJobResultPolicy.WAIT;
    }

    /**
     * Streamer broadcast job.
     */
    private static class StreamerBroadcastJob extends GridComputeJobAdapter implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Injected grid. */
        @GridInstanceResource
        private Grid g;

        /** Closure. */
        private GridInClosure<GridStreamerContext> clo;

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
        private StreamerBroadcastJob(GridInClosure<GridStreamerContext> clo, String streamer) {
            this.clo = clo;
            this.streamer = streamer;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws GridException {
            GridStreamer s = g.streamer(streamer);

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
            clo = (GridInClosure<GridStreamerContext>)in.readObject();
            streamer = U.readString(in);
        }
    }
}
