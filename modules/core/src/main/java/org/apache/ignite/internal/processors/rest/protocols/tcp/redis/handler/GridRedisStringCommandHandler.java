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

package org.apache.ignite.internal.processors.rest.protocols.tcp.redis.handler;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.CX1;

/**
 * Redis strings command handler.
 */
public abstract class GridRedisStringCommandHandler implements GridRedisCommandHandler {
    /** REST protocol handler. */
    protected GridRestProtocolHandler hnd;

    /**
     * Constructor.
     *
     * @param hnd REST protocol handler.
     */
    public GridRedisStringCommandHandler(GridRestProtocolHandler hnd) {
        this.hnd = hnd;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRedisMessage> handleAsync(GridRedisMessage msg) {
        assert msg != null;

        try {
            return hnd.handleAsync(asRestRequest(msg))
                .chain(new CX1<IgniteInternalFuture<GridRestResponse>, GridRedisMessage>() {
                    @Override
                    public GridRedisMessage applyx(IgniteInternalFuture<GridRestResponse> f)
                        throws IgniteCheckedException {
                        GridRestResponse restRes = f.get();

                        GridRedisMessage res = msg;

                        if (restRes.getSuccessStatus() == GridRestResponse.STATUS_SUCCESS) {
                            res.setResponse(makeResponse(restRes));
                        }
                        else
                            res.setResponse(GridRedisProtocolParser.toGenericError("Operation error!"));

                        return res;
                    }
                });
        }
        catch (IgniteCheckedException e) {
            msg.setResponse(GridRedisProtocolParser.toGenericError("Operation error!"));

            return new GridFinishedFuture<>(msg);
        }
    }

    /**
     * Converts {@link GridRedisMessage} to {@link GridRestRequest}.
     *
     * @param msg {@link GridRedisMessage}
     * @return {@link GridRestRequest}
     * @throws IgniteCheckedException If fails.
     */
    public abstract GridRestRequest asRestRequest(GridRedisMessage msg) throws IgniteCheckedException;

    /**
     * Prepares a response according to the request.
     *
     * @param resp REST response.
     * @return
     */
    public abstract ByteBuffer makeResponse(GridRestResponse resp);
}
