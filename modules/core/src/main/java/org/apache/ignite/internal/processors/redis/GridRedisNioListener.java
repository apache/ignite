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

package org.apache.ignite.internal.processors.redis;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.jetbrains.annotations.Nullable;

/**
 * Listener for Redis protocol requests.
 */
public class GridRedisNioListener extends GridNioServerListenerAdapter<GridRedisMessage> {
    /** Protocol handler. */
    private GridRedisProtocolHandler hnd;

    public GridRedisNioListener(GridRedisProtocolHandler hnd, GridKernalContext ctx) {
        this.hnd = hnd;
    }

    /** {@inheritDoc} */
    @Override public void onConnected(GridNioSession ses) {

    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
    }

    /** {@inheritDoc} */
    @Override public void onMessage(GridNioSession ses, GridRedisMessage msg) {
        IgniteInternalFuture<GridRedisMessage> f = hnd.handleAsync(msg);
        f.listen(new CIX1<IgniteInternalFuture<GridRedisMessage>>() {
            @Override public void applyx(IgniteInternalFuture<GridRedisMessage> f) throws IgniteCheckedException {
                GridRedisMessage restRes = f.get();
                sendResponse(ses, restRes);
            }
        });
    }

    /**
     * Sends a response to be decoded and sent to the Redis client.
     *
     * @param ses NIO session.
     * @param res Response.
     * @return NIO send future.
     */
    private GridNioFuture<?> sendResponse(GridNioSession ses, GridRedisMessage res) {
        return ses.send(res);
    }
}
