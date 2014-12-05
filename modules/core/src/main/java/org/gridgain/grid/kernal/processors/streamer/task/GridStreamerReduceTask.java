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
 * Streamer query task.
 */
@ComputeTaskNoResultCache
public class GridStreamerReduceTask<R1, R2> extends GridPeerDeployAwareTaskAdapter<Void, R2> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query closure. */
    private IgniteClosure<StreamerContext, R1> clos;

    /** Reducer. */
    private IgniteReducer<R1, R2> rdc;

    /** Streamer. */
    private String streamer;

    /**
     * @param clos Query closure.
     * @param rdc Query reducer.
     * @param streamer Streamer.
     */
    public GridStreamerReduceTask(IgniteClosure<StreamerContext, R1> clos, IgniteReducer<R1, R2> rdc,
        @Nullable String streamer) {
        super(U.peerDeployAware(clos));

        this.clos = clos;
        this.rdc = rdc;
        this.streamer = streamer;
    }

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Void arg)
        throws GridException {
        Map<ComputeJob, ClusterNode> res = U.newHashMap(subgrid.size());

        for (ClusterNode node : subgrid)
            res.put(new ReduceJob<>(clos, streamer), node);

        return res;
    }

    /** {@inheritDoc} */
    @Override public R2 reduce(List<ComputeJobResult> results) throws GridException {
        return rdc.reduce();
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws GridException {
        // No failover for this task.
        if (res.getException() != null)
            throw res.getException();

        rdc.collect(res.<R1>getData());

        return ComputeJobResultPolicy.WAIT;
    }

    /**
     * Query job.
     */
    private static class ReduceJob<R> extends ComputeJobAdapter implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Injected grid. */
        @IgniteInstanceResource
        private Ignite g;

        /** Query closure. */
        private IgniteClosure<StreamerContext, R> qryClos;

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
        private ReduceJob(IgniteClosure<StreamerContext, R> qryClos, String streamer) {
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
            qryClos = (IgniteClosure<StreamerContext, R>)in.readObject();
            streamer = U.readString(in);
        }
    }
}
