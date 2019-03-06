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
import org.apache.ignite.IgniteCheckedException;
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

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.GETRANGE;

/**
 * Redis SETRANGE command handler.
 */
public class GridRedisGetRangeCommandHandler extends GridRedisRestCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        GETRANGE
    );

    /** Start offset position in Redis message parameters. */
    private static final int START_OFFSET_POS = 1;

    /** End offset position in Redis message parameters. */
    private static final int END_OFFSET_POS = 2;

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Kernal context.
     */
    public GridRedisGetRangeCommandHandler(IgniteLogger log, GridRestProtocolHandler hnd, GridKernalContext ctx) {
        super(log, hnd, ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public GridRestRequest asRestRequest(GridRedisMessage msg) throws IgniteCheckedException {
        assert msg != null;

        if (msg.messageSize() < 4)
            throw new GridRedisGenericException("Wrong number of arguments");

        GridRestCacheRequest getReq = new GridRestCacheRequest();

        getReq.clientId(msg.clientId());
        getReq.key(msg.key());
        getReq.command(CACHE_GET);
        getReq.cacheName(msg.cacheName());

        return getReq;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer makeResponse(final GridRestResponse restRes, List<String> params) {
        if (restRes.getResponse() == null)
            return GridRedisProtocolParser.toBulkString("");

        if (restRes.getResponse() instanceof String) {
            String res = String.valueOf(restRes.getResponse());
            int startOff;
            int endOff;

            try {
                startOff = boundedStartOffset(Integer.parseInt(params.get(START_OFFSET_POS)), res.length());
                endOff = boundedEndOffset(Integer.parseInt(params.get(END_OFFSET_POS)), res.length());
            }
            catch (NumberFormatException e) {
                U.error(log, "Erroneous offset", e);

                return GridRedisProtocolParser.toGenericError("Offset is not an integer");
            }

            return GridRedisProtocolParser.toBulkString(res.substring(startOff, endOff));
        }
        else
            return GridRedisProtocolParser.toTypeError("Operation against a key holding the wrong kind of value");
    }

    /**
     * @param idx Index.
     * @param size Bounds.
     * @return Offset within the bounds.
     */
    private int boundedStartOffset(int idx, int size) {
        return idx >= 0 ? Math.min(idx, size) : size + idx;
    }

    /**
     * @param idx Index.
     * @param size Bounds.
     * @return Offset within the bounds.
     */
    private int boundedEndOffset(int idx, int size) {
        return idx >= 0 ? Math.min(idx + 1, size) : size + idx + 1;
    }
}
