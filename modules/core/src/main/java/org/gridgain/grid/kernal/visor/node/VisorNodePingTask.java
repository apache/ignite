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

package org.gridgain.grid.kernal.visor.node;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Ping other node.
 */
@GridInternal
public class VisorNodePingTask extends VisorOneNodeTask<UUID, GridTuple3<Boolean, Long, Long>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorPingJob job(UUID arg) {
        return new VisorPingJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected GridTuple3<Boolean, Long, Long> reduce0(List<ComputeJobResult> results) throws IgniteCheckedException {
        try {
            return super.reduce0(results);
        }
        catch (ClusterTopologyException ignored) {
            return new GridTuple3<>(false, -1L, -1L);
        }
    }

    /**
     * Job that ping node.
     */
    private static class VisorPingJob extends VisorJob<UUID, GridTuple3<Boolean, Long, Long>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Node ID to ping.
         * @param debug Debug flag.
         */
        protected VisorPingJob(UUID arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected GridTuple3<Boolean, Long, Long> run(UUID nodeToPing) throws IgniteCheckedException {
            long start = System.currentTimeMillis();

            return new GridTuple3<>(g.pingNode(nodeToPing), start, System.currentTimeMillis());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorPingJob.class, this);
        }
    }
}
