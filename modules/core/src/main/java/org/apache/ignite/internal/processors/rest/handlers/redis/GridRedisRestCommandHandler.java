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

package org.apache.ignite.internal.processors.rest.handlers.redis;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.redis.exception.GridRedisGenericException;
import org.apache.ignite.internal.processors.rest.handlers.redis.exception.GridRedisTypeException;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.CX1;
import org.jetbrains.annotations.Nullable;

/**
 * Redis command handler done via REST.
 */
public abstract class GridRedisRestCommandHandler implements GridRedisCommandHandler {
    /** Logger. */
    protected final IgniteLogger log;

    /** REST protocol handler. */
    protected final GridRestProtocolHandler hnd;

    /**
     * Constructor.
     *
     * @param log Logger.
     * @param hnd REST protocol handler.
     */
    public GridRedisRestCommandHandler(final IgniteLogger log, final GridRestProtocolHandler hnd) {
        this.log = log;
        this.hnd = hnd;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRedisMessage> handleAsync(final GridNioSession ses,
        final GridRedisMessage msg) {
        assert msg != null;

        try {
            return hnd.handleAsync(asRestRequest(msg))
                .chain(new CX1<IgniteInternalFuture<GridRestResponse>, GridRedisMessage>() {
                    @Override public GridRedisMessage applyx(IgniteInternalFuture<GridRestResponse> f)
                        throws IgniteCheckedException {
                        GridRestResponse restRes = f.get();

                        if (restRes.getSuccessStatus() == GridRestResponse.STATUS_SUCCESS)
                            msg.setResponse(makeResponse(restRes, msg.auxMKeys()));
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
     * Retrieves long value following the parameter name from parameters list.
     *
     * @param name Parameter name.
     * @param params Parameters list.
     * @return Long value from parameters list or null if not exists.
     * @throws GridRedisGenericException If parsing failed.
     */
    @Nullable protected Long longValue(String name, List<String> params) throws GridRedisGenericException {
        assert name != null;

        Iterator<String> it = params.iterator();

        while (it.hasNext()) {
            if (name.equalsIgnoreCase(it.next())) {
                if (it.hasNext()) {
                    String val = it.next();

                    try {
                        return Long.valueOf(val);
                    }
                    catch (NumberFormatException ignore) {
                        throw new GridRedisGenericException("Failed to parse parameter of Long type [" + name + "=" + val + "]");
                    }
                }
                else
                    throw new GridRedisGenericException("Syntax error. Missing value for parameter: " + name);
            }
        }

        return null;
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
     * @param params Auxiliary parameters.
     * @return Response for the command.
     */
    public abstract ByteBuffer makeResponse(GridRestResponse resp, List<String> params);
}
