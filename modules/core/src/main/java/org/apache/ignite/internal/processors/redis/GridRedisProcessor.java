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
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.redis.handler.GridRedisConnectionCommandHandler;

/**
 * Redis processor implementation.
 */
public class GridRedisProcessor extends GridProcessorAdapter {
    /** Server. */
    private GridRedisServer protoSrv;

    /** Protocol handler. */
    private GridRedisProtocolHandler hnd;

    /**
     * @param ctx Kernal context.
     */
    public GridRedisProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (!isRedisEnabled())
            return;

        hnd = new GridRedisProtocolHandler(ctx, log);

        hnd.addCommandHandler(new GridRedisConnectionCommandHandler());

        protoSrv = new GridRedisServer(ctx);

        protoSrv.start(hnd);

        if (log.isDebugEnabled())
            log.debug("Enabled Redis protocol: " + protoSrv);
    }

    /**
     * @return Whether or not Redis protocol is enabled.
     */
    private boolean isRedisEnabled() {
        return !ctx.config().isDaemon() && ctx.config().getRedisConfiguration() != null;
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        super.onKernalStart();

        if (!isRedisEnabled())
            return;

        hnd.start();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        super.onKernalStop(cancel);

        if (!isRedisEnabled())
            return;

        if (hnd != null)
            hnd.stopGracefully();

        if (protoSrv != null)
            protoSrv.stop();
    }
}
