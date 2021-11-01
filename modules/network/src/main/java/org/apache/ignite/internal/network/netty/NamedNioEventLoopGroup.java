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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.FastThreadLocalThread;

/**
 * Named netty event loop.
 */
public class NamedNioEventLoopGroup extends NioEventLoopGroup {
    /**
     * Constructor.
     *
     * @param threadFactory Thread factory.
     */
    private NamedNioEventLoopGroup(ThreadFactory threadFactory) {
        super(threadFactory);
    }

    /**
     * Creates event loop.
     *
     * @param namePrefix Tread name prefix.
     * @return Event loop.
     */
    public static NioEventLoopGroup create(String namePrefix) {
        var factory = new DefaultThreadFactory(namePrefix, Thread.MAX_PRIORITY) {
            /** Thread index. */
            private final AtomicInteger nextId = new AtomicInteger();

            /** {@inheritDoc} */
            @Override protected Thread newThread(Runnable r, String unused) {
                return new FastThreadLocalThread(threadGroup, r, namePrefix + '-' + nextId.incrementAndGet());
            }
        };
        return new NamedNioEventLoopGroup(factory);
    }
}
