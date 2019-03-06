/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.rest.handlers.redis.string;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.redis.GridRedisRestCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.exception.GridRedisGenericException;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT_IF_ABSENT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REPLACE;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.SET;

/**
 * Redis SET command handler.
 * <p>
 * No key expiration is currently supported.
 */
public class GridRedisSetCommandHandler extends GridRedisRestCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        SET
    );

    /** Value position in Redis message. */
    private static final int VAL_POS = 2;

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Kernal context.
     */
    public GridRedisSetCommandHandler(IgniteLogger log, GridRestProtocolHandler hnd, GridKernalContext ctx) {
        super(log, hnd, ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public GridRestRequest asRestRequest(GridRedisMessage msg) throws IgniteCheckedException {
        assert msg != null;

        if (msg.messageSize() < 3)
            throw new GridRedisGenericException("Wrong number of arguments");

        // check if an atomic long with the key exists (related to incr/decr).
        IgniteAtomicLong l = ctx.grid().atomicLong(msg.key(), 0, false);

        if (l != null) {
            try {
                l.close();
            }
            catch (IgniteException ignored) {
                U.warn(log, "Failed to remove atomic long for key: " + msg.key());
            }
        }

        GridRestCacheRequest restReq = new GridRestCacheRequest();

        restReq.clientId(msg.clientId());
        restReq.key(msg.key());

        restReq.command(CACHE_PUT);
        restReq.cacheName(msg.cacheName());

        restReq.value(msg.aux(VAL_POS));

        if (msg.messageSize() >= 4) {
            List<String> params = msg.aux();

            // get rid of SET value.
            params.remove(0);

            if (U.containsStringCollection(params, "nx", true))
                restReq.command(CACHE_PUT_IF_ABSENT);
            else if (U.containsStringCollection(params, "xx", true))
                restReq.command(CACHE_REPLACE);

            setExpire(restReq, params);
        }

        return restReq;
    }

    /**
     * Attempts to set expiration when EX or PX parameters are specified.
     *
     * @param restReq {@link GridRestCacheRequest}.
     * @param params Command parameters.
     * @throws GridRedisGenericException When parameters are not valid.
     */
    private void setExpire(GridRestCacheRequest restReq, List<String> params) throws GridRedisGenericException {
        Long px = longValue("px", params);
        Long ex = longValue("ex", params);

        if (px != null)
            restReq.ttl(px);
        else if (ex != null)
            restReq.ttl(ex * 1000L);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer makeResponse(final GridRestResponse restRes, List<String> params) {
        Object resp = restRes.getResponse();

        if (resp == null)
            return GridRedisProtocolParser.nil();

        return (!(boolean)resp ? GridRedisProtocolParser.nil() : GridRedisProtocolParser.oKString());
    }
}
