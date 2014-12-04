/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer.task;

import org.apache.ignite.*;
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
 * Streamer query task.
 */
@GridComputeTaskNoResultCache
public class GridStreamerReduceTask<R1, R2> extends GridPeerDeployAwareTaskAdapter<Void, R2> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query closure. */
    private GridClosure<GridStreamerContext, R1> clos;

    /** Reducer. */
    private GridReducer<R1, R2> rdc;

    /** Streamer. */
    private String streamer;

    /**
     * @param clos Query closure.
     * @param rdc Query reducer.
     * @param streamer Streamer.
     */
    public GridStreamerReduceTask(GridClosure<GridStreamerContext, R1> clos, GridReducer<R1, R2> rdc,
        @Nullable String streamer) {
        super(U.peerDeployAware(clos));

        this.clos = clos;
        this.rdc = rdc;
        this.streamer = streamer;
    }

    /** {@inheritDoc} */
    @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, @Nullable Void arg)
        throws GridException {
        Map<GridComputeJob, GridNode> res = U.newHashMap(subgrid.size());

        for (GridNode node : subgrid)
            res.put(new ReduceJob<>(clos, streamer), node);

        return res;
    }

    /** {@inheritDoc} */
    @Override public R2 reduce(List<GridComputeJobResult> results) throws GridException {
        return rdc.reduce();
    }

    /** {@inheritDoc} */
    @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd) throws GridException {
        // No failover for this task.
        if (res.getException() != null)
            throw res.getException();

        rdc.collect(res.<R1>getData());

        return GridComputeJobResultPolicy.WAIT;
    }

    /**
     * Query job.
     */
    private static class ReduceJob<R> extends GridComputeJobAdapter implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Injected grid. */
        @GridInstanceResource
        private Ignite g;

        /** Query closure. */
        private GridClosure<GridStreamerContext, R> qryClos;

        /** Streamer. */
        private String streamer;

        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public ReduceJob() {
            // No-op.
        }

        /**
         * @param qryClos Query closure.
         * @param streamer Streamer.
         */
        private ReduceJob(GridClosure<GridStreamerContext, R> qryClos, String streamer) {
            this.qryClos = qryClos;
            this.streamer = streamer;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws GridException {
            GridStreamer s = g.streamer(streamer);

            assert s != null;

            return qryClos.apply(s.context());
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(qryClos);
            U.writeString(out, streamer);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            qryClos = (GridClosure<GridStreamerContext, R>)in.readObject();
            streamer = U.readString(in);
        }
    }
}
