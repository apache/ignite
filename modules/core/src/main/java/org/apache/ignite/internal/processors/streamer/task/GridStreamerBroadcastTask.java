/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.streamer.task;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.streamer.*;
import org.apache.ignite.internal.processors.closure.*;
import org.apache.ignite.internal.util.typedef.internal.*;
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
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Void arg) {
        Map<ComputeJob, ClusterNode> res = U.newHashMap(subgrid.size());

        for (ClusterNode node : subgrid)
            res.put(new StreamerBroadcastJob(clo, streamer), node);

        return res;
    }

    /** {@inheritDoc} */
    @Override public Void reduce(List<ComputeJobResult> results) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
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
        @Override public Object execute() {
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
