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

package org.apache.ignite.internal.processors.cluster;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.lang.IgniteFuture;

/**
 *
 */
public class ClusterProcessor extends GridProcessorAdapter {
    /** */
    private IgniteClusterImpl cluster;

    /**
     * @param ctx Kernal context.
     */
    public ClusterProcessor(GridKernalContext ctx) {
        super(ctx);

        cluster = new IgniteClusterImpl(ctx);
    }

    /**
     * @return Cluster.
     */
    public IgniteClusterImpl get() {
        return cluster;
    }

    /**
     * @return Client reconnect future.
     */
    public IgniteFuture<?> clientReconnectFuture() {
        IgniteFuture<?> fut = cluster.clientReconnectFuture();

        return fut != null ? fut : new IgniteFinishedFutureImpl<>();
    }
}