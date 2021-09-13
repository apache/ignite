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

package org.apache.ignite.rest.netty;

import io.netty.buffer.EmptyByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.rest.routes.Router;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

/**
 * Main handler of REST HTTP chain.
 * It receives http request, process it by {@link Router} and produce http response.
 */
public class RestApiHandler extends SimpleChannelInboundHandler<HttpObject> {
    /** Ignite logger. */
    private final IgniteLogger log = IgniteLogger.forClass(getClass());

    /** Requests' router. */
    private final Router router;

    /**
     * Creates a new instance of API handler.
     *
     * @param router Router.
     */
    public RestApiHandler(Router router) {
        this.router = router;
    }

    /** {@inheritDoc} */
    @Override public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    /** {@inheritDoc} */
    @Override protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
        if (msg instanceof FullHttpRequest) {
            FullHttpRequest req = (FullHttpRequest) msg;
            FullHttpResponse res;

            var maybeRoute = router.route(req);
            if (maybeRoute.isPresent()) {
                var resp = new RestApiHttpResponse(new DefaultHttpResponse(HttpVersion.HTTP_1_1, OK));
                maybeRoute.get().handle(req, resp);
                var content = resp.content() != null ?
                    Unpooled.wrappedBuffer(resp.content()) :
                    new EmptyByteBuf(UnpooledByteBufAllocator.DEFAULT);
                res = new DefaultFullHttpResponse(resp.protocolVersion(), resp.status(),
                     content, resp.headers(), EmptyHttpHeaders.INSTANCE);
            }
            else
                res = new DefaultFullHttpResponse(req.protocolVersion(), HttpResponseStatus.NOT_FOUND);

            res.headers()
                .setInt(CONTENT_LENGTH, res.content().readableBytes());

            boolean keepAlive = HttpUtil.isKeepAlive(req);
            if (keepAlive) {
                if (!req.protocolVersion().isKeepAliveDefault())
                    res.headers().set(CONNECTION, KEEP_ALIVE);
            } else
                res.headers().set(CONNECTION, CLOSE);

            ChannelFuture f = ctx.write(res);

            if (!keepAlive)
                f.addListener(ChannelFutureListener.CLOSE);
        }

    }

    /** {@inheritDoc} */
    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Failed to process http request:", cause);
        var res = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        ctx.write(res).addListener(ChannelFutureListener.CLOSE);
    }
}
