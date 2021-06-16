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

package org.apache.ignite.internal.network.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Netty utilities.
 */
public class NettyUtils {
    /**
     * Convert a Netty {@link Future} to a {@link CompletableFuture}.
     *
     * @param nettyFuture Netty future.
     * @param mapper Function that maps successfully resolved Netty future to a value for a CompletableFuture.
     * @param <T> Resulting future type.
     * @param <R> Netty future result type.
     * @param <F> Netty future type.
     * @return CompletableFuture.
     */
    public static <T, R, F extends Future<R>> CompletableFuture<T> toCompletableFuture(
        F nettyFuture,
        Function<F, T> mapper
    ) {
        var fut = new CompletableFuture<T>();

        nettyFuture.addListener((F future) -> {
           if (future.isSuccess())
               fut.complete(mapper.apply(future));
           else if (future.isCancelled())
               fut.cancel(true);
           else
               fut.completeExceptionally(future.cause());
        });

        return fut;
    }

    /**
     * Convert a Netty {@link Future} to a {@link CompletableFuture}.
     *
     * @param <T> Type of the future.
     * @param future Future.
     * @return CompletableFuture.
     */
    public static <T> CompletableFuture<T> toCompletableFuture(Future<T> future) {
        return toCompletableFuture(future, fut -> null);
    }

    /**
     * Convert a Netty {@link ChannelFuture} to a {@link CompletableFuture}.
     *
     * @param channelFuture Channel future.
     * @return CompletableFuture.
     */
    public static CompletableFuture<Channel> toChannelCompletableFuture(ChannelFuture channelFuture) {
        return toCompletableFuture(channelFuture, ChannelFuture::channel);
    }
}
