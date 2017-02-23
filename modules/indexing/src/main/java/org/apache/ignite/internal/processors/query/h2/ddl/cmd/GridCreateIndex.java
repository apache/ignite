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

package org.apache.ignite.internal.processors.query.h2.ddl.cmd;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.h2.ddl.CreateIndexArguments;

/**
 * {@code CREATE INDEX} handler.
 */
public class GridCreateIndex implements GridDdlCommand<CreateIndexArguments> {
    /** Singleton. */
    public final static GridCreateIndex INSTANCE = new GridCreateIndex();

    /** */
    private GridCreateIndex() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> filterNodes(GridKernalContext kernalCtx, CreateIndexArguments args,
        AffinityTopologyVersion topVer) {
        return kernalCtx.discovery().cacheNodes(args.cacheName(), topVer);
    }

    /** {@inheritDoc} */
    @Override public void init(CreateIndexArguments cmdArgs) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void execute(CreateIndexArguments cmdArgs) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void cancel(CreateIndexArguments cmdArgs) throws IgniteCheckedException {
        // No-op.
    }
}
