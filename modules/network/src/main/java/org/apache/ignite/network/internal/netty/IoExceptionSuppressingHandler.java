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

package org.apache.ignite.network.internal.netty;

import java.io.IOException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.ignite.lang.IgniteLogger;

/**
 * Netty handler for suppressing IO exceptions that can happen if a remote peer abruptly closes the connection.
 */
class IoExceptionSuppressingHandler extends ChannelInboundHandlerAdapter {
    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(IoExceptionSuppressingHandler.class);

    /** {@inheritDoc} */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof IOException)
            LOG.debug(cause.getMessage(), cause);
        else
            ctx.fireExceptionCaught(cause);
    }
}
