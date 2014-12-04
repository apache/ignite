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
import org.apache.ignite.lang.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.closure.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Streamer query task.
 */
public class GridStreamerQueryTask<R> extends GridPeerDeployAwareTaskAdapter<Void, Collection<R>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query closure. */
    private IgniteClosure<GridStreamerContext, R> qryClos;

    /** Streamer. */
    private String streamer;

    /**
     * @param qryClos Query closure.
     * @param streamer Streamer.
     */
    public GridStreamerQueryTask(IgniteClosure<GridStreamerContext, R> qryClos, @Nullable String streamer) {
        super(U.peerDeployAware(qryClos));

        this.qryClos = qryClos;
        this.streamer = streamer;
    }

    /** {@inheritDoc} */
    @Override public Map<? extends GridComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Void arg)
        throws GridException {
        Map<GridComputeJob, ClusterNode> res = U.newHashMap(subgrid.size());

        for (ClusterNode node : subgrid)
            res.put(new QueryJob<>(qryClos, streamer), node);

        return res;
    }

    /** {@inheritDoc} */
    @Override public Collection<R> reduce(List<GridComputeJobResult> results) throws GridException {
        Collection<R> res = new ArrayList<>(results.size());

        for (GridComputeJobResult jobRes : results)
            res.add(jobRes.<R>getData());

        return res;
    }

    /** {@inheritDoc} */
    @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd) throws GridException {
        // No failover for this task.
        if (res.getException() != null)
            throw res.getException();

        return GridComputeJobResultPolicy.WAIT;
    }

    /**
     * Query job.
     */
    private static class QueryJob<R> extends GridComputeJobAdapter implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Injected grid. */
        @GridInstanceResource
        private Ignite g;

        /** Query closure. */
        private IgniteClosure<GridStreamerContext, R> qryClos;

        /** Streamer. */
        private String streamer;

        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public QueryJob() {
            // No-op.
        }

        /**
         * @param qryClos Query closure.
         * @param streamer Streamer.
         */
        private QueryJob(IgniteClosure<GridStreamerContext, R> qryClos, String streamer) {
            this.qryClos = qryClos;
            this.streamer = streamer;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws GridException {
            IgniteStreamer s = g.streamer(streamer);

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
            qryClos = (IgniteClosure<GridStreamerContext, R>)in.readObject();
            streamer = U.readString(in);
        }
    }
}
