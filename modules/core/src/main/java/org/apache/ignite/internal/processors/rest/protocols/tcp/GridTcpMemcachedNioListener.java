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

package org.apache.ignite.internal.processors.rest.protocols.tcp;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.cache.GridCacheRestMetrics;
import org.apache.ignite.internal.processors.rest.request.DataStructuresRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridEmbeddedFuture;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.C2;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.ATOMIC_DECREMENT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.ATOMIC_INCREMENT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_ADD;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_APPEND;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_METRICS;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PREPEND;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REMOVE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REMOVE_ALL;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REPLACE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.NOOP;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.QUIT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.VERSION;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.GridMemcachedMessage.FAILURE;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.GridMemcachedMessage.KEY_NOT_FOUND;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.GridMemcachedMessage.MEMCACHE_REQ_FLAG;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.GridMemcachedMessage.SUCCESS;
import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.LAST_FUT;

/**
 * Handles memcache requests.
 */
public class GridTcpMemcachedNioListener extends GridNioServerListenerAdapter<GridMemcachedMessage> {
    /** Logger */
    private final IgniteLogger log;

    /** Handler. */
    private final GridRestProtocolHandler hnd;

    /**
     * Creates listener which will convert incoming tcp packets to rest requests and forward them to
     * a given rest handler.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     */
    public GridTcpMemcachedNioListener(IgniteLogger log, GridRestProtocolHandler hnd) {
        this.log = log;
        this.hnd = hnd;
    }

    /** {@inheritDoc} */
    @Override public void onConnected(GridNioSession ses) {
        // No-op, never called.
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
        // No-op, never called.
        assert false;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional"})
    @Override public void onMessage(final GridNioSession ses, final GridMemcachedMessage req) {
        assert req != null;

        final GridTuple3<GridRestCommand, Boolean, Boolean> cmd = command(req.operationCode());

        if (cmd == null) {
            U.warn(log, "Cannot find corresponding REST command for op code (session will be closed) [ses=" + ses +
                ", opCode=" + Integer.toHexString(req.operationCode()) + ']');

            ses.close();

            return;
        }

        assert req.requestFlag() == MEMCACHE_REQ_FLAG;
        assert cmd.get2() != null && cmd.get3() != null;

        // Close connection on 'Quit' command.
        if (cmd.get1() == QUIT) {
            try {
                if (cmd.get2()) {
                    GridMemcachedMessage res = new GridMemcachedMessage(req);

                    sendResponse(ses, res).get();
                }
            }
            // Catch all when quitting.
            catch (Exception e) {
                U.warn(log, "Failed to send quit response packet (session will be closed anyway) [ses=" + ses +
                    ", msg=" + e.getMessage() + "]");
            }
            finally {
                ses.close();
            }

            return;
        }

        IgniteInternalFuture<GridRestResponse> lastFut = ses.removeMeta(LAST_FUT.ordinal());

        if (lastFut != null && lastFut.isDone())
            lastFut = null;

        IgniteInternalFuture<GridRestResponse> f;

        if (lastFut == null)
            f = handleRequest0(ses, req, cmd);
        else {
            f = new GridEmbeddedFuture<>(
                lastFut,
                new C2<GridRestResponse, Exception, IgniteInternalFuture<GridRestResponse>>() {
                    @Override public IgniteInternalFuture<GridRestResponse> apply(GridRestResponse res, Exception e) {
                        return handleRequest0(ses, req, cmd);
                    }
                }
            );
        }

        if (f != null)
            ses.addMeta(LAST_FUT.ordinal(), f);
    }

    /**
     * @param ses Session.
     * @param req Request.
     * @param cmd Command.
     * @return Future or {@code null} if processed immediately.
     */
    @Nullable private IgniteInternalFuture<GridRestResponse> handleRequest0(
        final GridNioSession ses,
        final GridMemcachedMessage req,
        final GridTuple3<GridRestCommand, Boolean, Boolean> cmd
    ) {
        if (cmd.get1() == NOOP) {
            GridMemcachedMessage res0 = new GridMemcachedMessage(req);

            res0.status(SUCCESS);

            sendResponse(ses, res0);

            return null;
        }

        IgniteInternalFuture<GridRestResponse> f = hnd.handleAsync(createRestRequest(req, cmd.get1()));

        f.listen(new CIX1<IgniteInternalFuture<GridRestResponse>>() {
            @Override public void applyx(IgniteInternalFuture<GridRestResponse> f) throws IgniteCheckedException {
                GridRestResponse restRes = f.get();

                // Handle 'Stat' command (special case because several packets are included in response).
                if (cmd.get1() == CACHE_METRICS) {
                    assert restRes.getResponse() instanceof GridCacheRestMetrics;

                    Map<String, Long> metrics = ((GridCacheRestMetrics)restRes.getResponse()).map();

                    for (Map.Entry<String, Long> e : metrics.entrySet()) {
                        GridMemcachedMessage res = new GridMemcachedMessage(req);

                        res.key(e.getKey());

                        res.value(String.valueOf(e.getValue()));

                        sendResponse(ses, res);
                    }

                    sendResponse(ses, new GridMemcachedMessage(req));
                }
                else {
                    GridMemcachedMessage res = new GridMemcachedMessage(req);

                    if (restRes.getSuccessStatus() == GridRestResponse.STATUS_SUCCESS) {
                        switch (cmd.get1()) {
                            case CACHE_GET: {
                                res.status(restRes.getResponse() == null ? KEY_NOT_FOUND : SUCCESS);

                                break;
                            }

                            case CACHE_PUT:
                            case CACHE_ADD:
                            case CACHE_REMOVE:
                            case CACHE_REPLACE:
                            case CACHE_CAS:
                            case CACHE_APPEND:
                            case CACHE_PREPEND: {
                                boolean res0 = restRes.getResponse().equals(Boolean.TRUE);

                                res.status(res0 ? SUCCESS : FAILURE);

                                break;
                            }

                            default: {
                                res.status(SUCCESS);

                                break;
                            }
                        }
                    }
                    else
                        res.status(FAILURE);

                    if (cmd.get3())
                        res.key(req.key());

                    if (restRes.getSuccessStatus() == GridRestResponse.STATUS_SUCCESS && res.addData() &&
                        restRes.getResponse() != null)
                        res.value(restRes.getResponse());

                    sendResponse(ses, res);
                }
            }
        });

        return f;
    }

    /**
     * @param ses NIO session.
     * @param res Response.
     * @return NIO send future.
     */
    private GridNioFuture<?> sendResponse(GridNioSession ses, GridMemcachedMessage res) {
        return ses.send(res);
    }

    /**
     * Creates REST request from the protocol request.
     *
     * @param req Request.
     * @param cmd Command.
     * @return REST request.
     */
    @SuppressWarnings("unchecked")
    private GridRestRequest createRestRequest(GridMemcachedMessage req, GridRestCommand cmd) {
        assert req != null;

        if (cmd == ATOMIC_INCREMENT || cmd == ATOMIC_DECREMENT) {
            DataStructuresRequest restReq = new DataStructuresRequest();

            restReq.command(cmd);
            restReq.key(req.key());
            restReq.delta(req.delta());
            restReq.initial(req.initial());

            return restReq;
        }
        else {
            GridRestCacheRequest restReq = new GridRestCacheRequest();

            restReq.command(cmd);
            restReq.clientId(req.clientId());
            restReq.ttl(req.expiration());
            restReq.cacheName(req.cacheName());
            restReq.key(req.key());

            if (cmd == CACHE_REMOVE_ALL) {
                Object[] keys = (Object[]) req.value();

                if (keys != null) {
                    Map<Object, Object> map = new HashMap<>();

                    for (Object key : keys) {
                        map.put(key, null);
                    }

                    restReq.values(map);
                }
            }
            else {
                if (req.value() != null)
                    restReq.value(req.value());
            }

            return restReq;
        }
    }

    /**
     * Gets command and command attributes from operation code.
     *
     * @param opCode Operation code.
     * @return Command.
     */
    @Nullable private GridTuple3<GridRestCommand, Boolean, Boolean> command(int opCode) {
        GridRestCommand cmd;
        boolean quiet = false;
        boolean retKey = false;

        switch (opCode) {
            case 0x00:
                cmd = CACHE_GET;

                break;
            case 0x01:
                cmd = CACHE_PUT;

                break;
            case 0x02:
                cmd = CACHE_ADD;

                break;
            case 0x03:
                cmd = CACHE_REPLACE;

                break;
            case 0x04:
                cmd = CACHE_REMOVE;

                break;
            case 0x05:
                cmd = ATOMIC_INCREMENT;

                break;
            case 0x06:
                cmd = ATOMIC_DECREMENT;

                break;
            case 0x07:
                cmd = QUIT;

                break;
            case 0x08:
                cmd = CACHE_REMOVE_ALL;

                break;
            case 0x09:
                cmd = CACHE_GET;

                break;
            case 0x0A:
                cmd = NOOP;

                break;
            case 0x0B:
                cmd = VERSION;

                break;
            case 0x0C:
                cmd = CACHE_GET;
                retKey = true;

                break;
            case 0x0D:
                cmd = CACHE_GET;
                retKey = true;

                break;
            case 0x0E:
                cmd = CACHE_APPEND;

                break;
            case 0x0F:
                cmd = CACHE_PREPEND;

                break;
            case 0x10:
                cmd = CACHE_METRICS;

                break;
            case 0x11:
                cmd = CACHE_PUT;
                quiet = true;

                break;
            case 0x12:
                cmd = CACHE_ADD;
                quiet = true;

                break;
            case 0x13:
                cmd = CACHE_REPLACE;
                quiet = true;

                break;
            case 0x14:
                cmd = CACHE_REMOVE;
                quiet = true;

                break;
            case 0x15:
                cmd = ATOMIC_INCREMENT;
                quiet = true;

                break;
            case 0x16:
                cmd = ATOMIC_DECREMENT;
                quiet = true;

                break;
            case 0x17:
                cmd = QUIT;
                quiet = true;

                break;
            case 0x18:
                cmd = CACHE_REMOVE_ALL;
                quiet = true;

                break;
            case 0x19:
                cmd = CACHE_APPEND;
                quiet = true;

                break;
            case 0x1A:
                cmd = CACHE_PREPEND;
                quiet = true;

                break;
            default:
                return null;
        }

        return new GridTuple3<>(cmd, quiet, retKey);
    }
}
