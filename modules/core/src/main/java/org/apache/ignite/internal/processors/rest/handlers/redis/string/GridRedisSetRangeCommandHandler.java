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
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.SETRANGE;

/**
 * Redis SETRANGE command handler.
 */
public class GridRedisSetRangeCommandHandler extends GridRedisRestCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        SETRANGE
    );

    /** Offset position in Redis message among parameters. */
    private static final int OFFSET_POS = 2;

    /** Value position in Redis message. */
    private static final int VAL_POS = 3;

    /** Maximum offset. */
    private static final int MAX_OFFSET = 536870911;

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     */
    public GridRedisSetRangeCommandHandler(final IgniteLogger log, final GridRestProtocolHandler hnd) {
        super(log, hnd);
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

        int off;

        try {
            off = Integer.parseInt(msg.aux(OFFSET_POS));
        }
        catch (NumberFormatException e) {
            U.error(log, "Erroneous offset", e);
            throw new GridRedisGenericException("Offset is not an integer");
        }

        String val = String.valueOf(msg.aux(VAL_POS));

        GridRestCacheRequest getReq = new GridRestCacheRequest();

        getReq.clientId(msg.clientId());
        getReq.key(msg.key());
        getReq.command(CACHE_GET);
        getReq.cacheName(msg.cacheName());

        if (val.isEmpty())
            return getReq;

        Object resp = hnd.handle(getReq).getResponse();

        int totalLen = off + val.length();

        if (off < 0 || totalLen > MAX_OFFSET)
            throw new GridRedisGenericException("Offset is out of range");

        GridRestCacheRequest putReq = new GridRestCacheRequest();

        putReq.clientId(msg.clientId());
        putReq.key(msg.key());
        putReq.command(CACHE_PUT);
        putReq.cacheName(msg.cacheName());

        if (resp == null) {
            byte[] dst = new byte[totalLen];

            System.arraycopy(val.getBytes(), 0, dst, off, val.length());

            putReq.value(new String(dst));
        }
        else {
            if (!(resp instanceof String))
                return getReq;

            String cacheVal = String.valueOf(resp);

            cacheVal = cacheVal.substring(0, off) + val;

            putReq.value(cacheVal);
        }

        hnd.handle(putReq);

        return getReq;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer makeResponse(final GridRestResponse restRes, List<String> params) {
        if (restRes.getResponse() == null)
            return GridRedisProtocolParser.toInteger("0");

        if (restRes.getResponse() instanceof String) {
            int resLen = ((String)restRes.getResponse()).length();
            return GridRedisProtocolParser.toInteger(String.valueOf(resLen));
        }
        else
            return GridRedisProtocolParser.toTypeError("Operation against a key holding the wrong kind of value");
    }
}
