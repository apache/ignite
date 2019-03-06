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

import java.util.Collection;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisNioListener;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.ECHO;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.PING;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.QUIT;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.SELECT;

/**
 * Redis connection handler.
 */
public class GridRedisConnectionCommandHandler implements GridRedisCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        PING,
        QUIT,
        ECHO,
        SELECT
    );

    /** Grid context. */
    private final GridKernalContext ctx;

    /** PONG response to PING. */
    private static final String PONG = "PONG";

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Context.
     */
    public GridRedisConnectionCommandHandler(final IgniteLogger log, final GridRestProtocolHandler hnd,
        GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRedisMessage> handleAsync(GridNioSession ses, GridRedisMessage msg) {
        assert msg != null;

        switch (msg.command()) {
            case PING:
                msg.setResponse(GridRedisProtocolParser.toSimpleString(PONG));

                return new GridFinishedFuture<>(msg);

            case QUIT:
                msg.setResponse(GridRedisProtocolParser.oKString());

                return new GridFinishedFuture<>(msg);

            case ECHO:
                msg.setResponse(GridRedisProtocolParser.toSimpleString(msg.key()));

                return new GridFinishedFuture<>(msg);

            case SELECT:
                String cacheIdx = msg.key();

                if (F.isEmpty(cacheIdx))
                    msg.setResponse(GridRedisProtocolParser.toGenericError("No cache index specified"));
                else {
                    String cacheName = GridRedisMessage.CACHE_NAME_PREFIX + "-" + cacheIdx;

                    CacheConfiguration ccfg = ctx.cache().cacheConfiguration(GridRedisMessage.DFLT_CACHE_NAME);
                    ccfg.setName(cacheName);

                    ctx.grid().getOrCreateCache(ccfg);

                    ses.addMeta(GridRedisNioListener.CONN_CTX_META_KEY, cacheName);

                    msg.setResponse(GridRedisProtocolParser.oKString());
                }
                return new GridFinishedFuture<>(msg);
        }

        return new GridFinishedFuture<>();
    }
}
