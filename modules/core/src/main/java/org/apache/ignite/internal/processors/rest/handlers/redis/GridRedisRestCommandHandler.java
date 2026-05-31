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

package org.apache.ignite.internal.processors.rest.handlers.redis;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.redis.exception.GridRedisGenericException;
import org.apache.ignite.internal.processors.rest.handlers.redis.exception.GridRedisTypeException;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisNioListener.SESS_TX_META_KEY;

/**
 * Redis command handler done via REST.
 */
public abstract class GridRedisRestCommandHandler implements GridRedisCommandHandler {
    /** Logger. */
    protected final IgniteLogger log;

    /** REST protocol handler. */
    protected final GridRestProtocolHandler hnd;

    /** Kernel context. */
    protected final GridKernalContext ctx;

    /**
     * Constructor.
     *
     * @param log Logger.
     * @param hnd REST protocol handler.
     * @param ctx Kernal context.
     */
    protected GridRedisRestCommandHandler(IgniteLogger log, GridRestProtocolHandler hnd, GridKernalContext ctx) {
        this.log = log;
        this.hnd = hnd;
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRedisMessage> handleAsync(final GridNioSession ses,
        final GridRedisMessage msg) {
        assert msg != null;

        try {
            trySuspendTransaction(ses,msg);
            return hnd.handleAsync(asRestRequest(msg))
                .chain(new CX1<IgniteInternalFuture<GridRestResponse>, GridRedisMessage>() {
                    @Override public GridRedisMessage applyx(IgniteInternalFuture<GridRestResponse> f)
                        throws IgniteCheckedException {
                        GridRestResponse restRes = f.get();
                        tryResumeTransaction(ses,msg);
                        if (restRes.getSuccessStatus() == GridRestResponse.STATUS_SUCCESS)
                            msg.setResponse(makeResponse(restRes, msg, msg.auxMKeys()));
                        else
                            msg.setResponse(GridRedisProtocolParser.toGenericError("Operation error"));

                        return msg;
                    }
                });
        }
        catch (IgniteCheckedException e) {
            if (e instanceof GridRedisTypeException)
                msg.setResponse(GridRedisProtocolParser.toTypeError(e.getMessage()));
            else
                msg.setResponse(GridRedisProtocolParser.toGenericError(e.getMessage()));

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

    public ByteBuffer makeResponse(GridRestResponse resp, GridRedisMessage msg,List<String> params){
        return makeResponse(resp,params);
    }

    /**
     * Prepares a response according to the request.
     *
     * @param resp REST response.
     * @param params Auxiliary parameters.
     * @return Response for the command.
     */
    public ByteBuffer makeResponse(GridRestResponse resp, List<String> params){
        if (resp.getError()!=null && !resp.getError().isEmpty())
            return GridRedisProtocolParser.toGenericError(resp.getError());

        if (resp.getResponse() == null)
            return GridRedisProtocolParser.nil();

        if (resp.getResponse() instanceof String)
            return GridRedisProtocolParser.toBulkString(resp.getResponse());
        else if (resp.getResponse() instanceof Number)
            return GridRedisProtocolParser.toInteger(resp.getResponse().toString());
        else if (resp.getResponse() instanceof Map)
            return GridRedisProtocolParser.toOrderedArray((Map<Object, Object>)resp.getResponse(), params);
        else if (resp.getResponse() instanceof Collection)
            return GridRedisProtocolParser.toArray((Collection)resp.getResponse());
        else
            throw new UnsupportedOperationException();

    }

    public boolean trySuspendTransaction(GridNioSession ses,GridRedisMessage msg){
        Transaction t = ses.meta(SESS_TX_META_KEY);
        if(t!=null) {
            boolean isTransCache = true;
            IgniteCache<?,?> stream = ctx.grid().cache(msg.cacheName());
            CacheConfiguration<?,?> cfg = stream.getConfiguration(CacheConfiguration.class);
            if(cfg.getAtomicityMode()!= CacheAtomicityMode.TRANSACTIONAL) {
                //this.log.warning("IgniteTransactions is only enable on CacheAtomicityMode.TRANSACTIONAL, cache. "+stream.getName()+" is not!");
                isTransCache = false;
            }
            if(!isTransCache) {
                t.suspend();
                return true;
            }
        }
        return false;
    }

    public boolean tryResumeTransaction(GridNioSession ses,GridRedisMessage msg){
        Transaction t = ses.meta(SESS_TX_META_KEY);
        if(t!=null) {
            boolean isTransCache = true;
            IgniteCache<?,?> stream = ctx.grid().cache(msg.cacheName());
            CacheConfiguration<?,?> cfg = stream.getConfiguration(CacheConfiguration.class);
            if(cfg.getAtomicityMode()!= CacheAtomicityMode.TRANSACTIONAL) {
                //this.log.warning("IgniteTransactions is only enable on CacheAtomicityMode.TRANSACTIONAL, cache. "+stream.getName()+" is not!");
                isTransCache = false;
            }
            if(!isTransCache) {
                t.resume();
                return true;
            }
        }
        return false;
    }
}
