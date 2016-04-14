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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET_ALL;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT_ALL;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.GET;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.MGET;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.MSET;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.SET;

/**
 * Redis strings command handler.
 */
public class GridRedisStringCommandHandler implements GridRedisCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        GET,
        MGET,
        SET,
        MSET
    );

    /** REST protocol handler. */
    private GridRestProtocolHandler hnd;

    /**
     * Constructor.
     *
     * @param hnd REST protocol handler.
     */
    public GridRedisStringCommandHandler(GridRestProtocolHandler hnd) {
        this.hnd = hnd;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRedisMessage> handleAsync(GridRedisMessage msg) {
        assert msg != null;

        try {
            return hnd.handleAsync(toRestRequest(msg))
                .chain(new CX1<IgniteInternalFuture<GridRestResponse>, GridRedisMessage>() {
                    @Override
                    public GridRedisMessage applyx(IgniteInternalFuture<GridRestResponse> f)
                        throws IgniteCheckedException {
                        GridRestResponse restRes = f.get();

                        GridRedisMessage res = msg;
                        ByteBuffer resp;

                        if (restRes.getSuccessStatus() == GridRestResponse.STATUS_SUCCESS) {
                            switch (res.command()) {
                                case GET:
                                    resp = (restRes.getResponse() == null ? GridRedisProtocolParser.nil()
                                        : GridRedisProtocolParser.toBulkString(restRes.getResponse()));

                                    break;
                                case MGET:
                                    resp = (restRes.getResponse() == null ? GridRedisProtocolParser.nil()
                                        : GridRedisProtocolParser.toArray((Map<Object, Object>)restRes.getResponse()));

                                    break;
                                case SET:
                                    resp = (restRes.getResponse() == null ? GridRedisProtocolParser.nil()
                                        : GridRedisProtocolParser.OkString());

                                    break;
                                case MSET:
                                    resp = GridRedisProtocolParser.OkString();

                                    break;

                                default:
                                    resp = GridRedisProtocolParser.toGenericError("Unsupported operation!");
                            }
                            res.setResponse(resp);
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
     * @param msg {@link GridRedisMessage}
     * @return {@link GridRestRequest}
     */
    private GridRestRequest toRestRequest(GridRedisMessage msg) throws IgniteCheckedException {
        assert msg != null;

        GridRestCacheRequest restReq = new GridRestCacheRequest();

        restReq.clientId(msg.clientId());
        restReq.key(msg.key());

        switch (msg.command()) {
            case SET:
                restReq.command(CACHE_PUT);

                if (msg.getMsgParts().size() < 3)
                    throw new IgniteCheckedException("Invalid request!");

                restReq.value(msg.getMsgParts().get(2));

                if (msg.getMsgParts().size() >= 4) {
                    // handle options.
                }

                break;

            case MSET:
                restReq.command(CACHE_PUT_ALL);

                List<String> els = msg.getMsgParts().subList(1, msg.getMsgParts().size());
                Map<Object, Object> mset = U.newHashMap(els.size() / 2);
                Iterator<String> msetIt = els.iterator();

                while (msetIt.hasNext())
                    mset.put(msetIt.next(), msetIt.hasNext() ? msetIt.next() : null);

                restReq.values(mset);

                break;

            case MGET:
                restReq.command(CACHE_GET_ALL);

                List<String> keys = msg.getMsgParts().subList(1, msg.getMsgParts().size());
                Map<Object, Object> mget = U.newHashMap(keys.size());
                Iterator<String> mgetIt = keys.iterator();

                while (mgetIt.hasNext())
                    mget.put(mgetIt.next(), null);

                restReq.values(mget);

                break;

            case GET:
                restReq.command(CACHE_GET);

                break;

            default:
                restReq.command(null);
        }

        return restReq;
    }
}
