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

package org.apache.ignite.internal.processors.rest.handlers.redis.server;

import java.util.Collection;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.handlers.redis.GridRedisCommandHandler;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.CLIENT;

/**
 * Redis CLIENT command handler.
 * <p>
 * CLIENT is a connection-scoped command container, so it is handled locally, without a REST round trip.
 * Only the subcommands that carry no cluster-wide state are supported, the rest are answered with an error.
 */
public class GridRedisClientCommandHandler implements GridRedisCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(CLIENT);

    /** Session metadata key for the name set by CLIENT SETNAME. */
    private static final int CLIENT_NAME_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Position of the first argument of a CLIENT subcommand. */
    private static final int ARG_POS = 2;

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRedisMessage> handleAsync(GridNioSession ses, GridRedisMessage msg) {
        assert msg != null;

        String subCmd = msg.key();

        if (F.isEmpty(subCmd)) {
            msg.setResponse(GridRedisProtocolParser.toGenericError(
                "wrong number of arguments for 'client' command"));

            return new GridFinishedFuture<>(msg);
        }

        switch (subCmd.toUpperCase()) {
            case "SETNAME": {
                String name = msg.aux(ARG_POS);

                if (name == null || name.indexOf(' ') >= 0 || name.indexOf('\n') >= 0)
                    msg.setResponse(GridRedisProtocolParser.toGenericError(
                        "Client names cannot contain spaces, newlines or special characters."));
                else {
                    ses.addMeta(CLIENT_NAME_META_KEY, name);

                    msg.setResponse(GridRedisProtocolParser.oKString());
                }

                break;
            }

            case "GETNAME": {
                String name = ses.meta(CLIENT_NAME_META_KEY);

                msg.setResponse(name == null
                    ? GridRedisProtocolParser.nil()
                    : GridRedisProtocolParser.toBulkString(name));

                break;
            }

            case "SETINFO":
                // The library name and version announced by a driver: accepted and ignored.
                msg.setResponse(GridRedisProtocolParser.oKString());

                break;

            default:
                msg.setResponse(GridRedisProtocolParser.toGenericError(
                    "Unknown subcommand '" + subCmd + "' for 'client' command"));
        }

        return new GridFinishedFuture<>(msg);
    }
}
