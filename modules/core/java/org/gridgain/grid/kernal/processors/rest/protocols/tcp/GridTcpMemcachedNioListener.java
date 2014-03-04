/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.protocols.tcp;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.handlers.cache.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.nio.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.kernal.processors.rest.GridRestCommand.*;
import static org.gridgain.grid.kernal.processors.rest.protocols.tcp.GridMemcachedMessage.*;

/**
 * Handles memcache requests.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridTcpMemcachedNioListener extends GridNioServerListenerAdapter<GridMemcachedMessage> {
    /** Logger */
    private final GridLogger log;

    /** Handler. */
    private final GridRestProtocolHandler hnd;

    /**
     * Creates listener which will convert incoming tcp packets to rest requests and forward them to
     * a given rest handler.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     */
    public GridTcpMemcachedNioListener(GridLogger log, GridRestProtocolHandler hnd) {
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
    @SuppressWarnings("ConstantConditions")
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

                    ses.send(res).get();
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

        if (cmd.get1() == NOOP) {
            GridMemcachedMessage res = new GridMemcachedMessage(req);

            res.status(SUCCESS);

            ses.send(res);

            return;
        }

        GridFuture<GridRestResponse> fut = hnd.handleAsync(createRestRequest(req, cmd.get1()));

        fut.listenAsync(new CIX1<GridFuture<GridRestResponse>>() {
            @Override public void applyx(GridFuture<GridRestResponse> f) throws GridException {
                GridRestResponse restRes = f.get();

                // Handle 'Stat' command (special case because several packets are included in response).
                if (cmd.get1() == CACHE_METRICS) {
                    assert restRes.getResponse() instanceof GridCacheRestMetrics;

                    Map<String, Long> metrics = ((GridCacheRestMetrics)restRes.getResponse()).map();

                    for (Map.Entry<String, Long> e : metrics.entrySet()) {
                        GridMemcachedMessage res = new GridMemcachedMessage(req);

                        res.key(e.getKey());

                        res.value(String.valueOf(e.getValue()));

                        ses.send(res);
                    }

                    ses.send(new GridMemcachedMessage(req));
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

                    ses.send(res);
                }
            }
        });
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

        GridRestRequest restReq = new GridRestRequest(cmd);

        restReq.setClientId(req.clientId());

        Map<String, Object> params = new GridLeanMap<>(6);

        Long exp = req.expiration();

        if (exp != null)
            params.put("exp", exp);

        Long delta = req.delta();

        if (delta != null)
            params.put("delta", delta);

        Long init = req.initial();

        if (init != null)
            params.put("init", init);

        params.put("cacheName", req.cacheName());

        if (req.key() != null)
            params.put("key", req.key());

        if (cmd == CACHE_REMOVE_ALL) {
            Object[] keys = (Object[])req.value();

            if (keys != null) {
                int i = 1;

                for (Object key : keys)
                    params.put("k" + i++, key);
            }
        }
        else {
            if (req.value() != null)
                params.put("val", req.value());
        }

        restReq.setParameters(params);

        return restReq;
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
                cmd = CACHE_INCREMENT;

                break;
            case 0x06:
                cmd = CACHE_DECREMENT;

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
                cmd = CACHE_INCREMENT;
                quiet = true;

                break;
            case 0x16:
                cmd = CACHE_DECREMENT;
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
