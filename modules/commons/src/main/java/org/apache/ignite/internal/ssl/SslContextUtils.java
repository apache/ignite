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

package org.apache.ignite.internal.ssl;

import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.jetbrains.annotations.Nullable;

/** Rebuilds the SSL context of a configured factory. */
public class SslContextUtils {
    /** */
    private SslContextUtils() {
        // No-op.
    }

    /**
     * Rebuilds the context and makes the factory hand it out from now on, which is how transports that ask the
     * factory per connection, such as outbound communication, pick the new certificates up.
     *
     * @param factory SSL context factory.
     * @param cur Context the caller is using, or {@code null} if it has none yet.
     * @return See {@link #rebuild(Factory, SSLContext, boolean)}.
     * @throws SSLException If the context could not be built.
     */
    @Nullable public static SSLContext reload(Factory<SSLContext> factory, @Nullable SSLContext cur)
        throws SSLException {
        return rebuild(factory, cur, true);
    }

    /**
     * Rebuilds the context and leaves the factory as it was, for callers that only want to know whether the stores
     * on disk can be used, or that apply the context themselves. A factory shared with someone else is then left
     * serving what it served before.
     *
     * @param factory SSL context factory.
     * @param cur Context the caller is using, or {@code null} if it has none yet.
     * @return See {@link #rebuild(Factory, SSLContext, boolean)}.
     * @throws SSLException If the context could not be built.
     */
    @Nullable public static SSLContext build(Factory<SSLContext> factory, @Nullable SSLContext cur)
        throws SSLException {
        return rebuild(factory, cur, false);
    }

    /**
     * @param factory SSL context factory.
     * @param cur Context the caller is using, or {@code null} if it has none yet.
     * @param apply Whether the factory should hand the rebuilt context out afterwards.
     * @return Rebuilt context, or {@code null} if the factory returned the context already in use: such a factory
     *      caches it internally and cannot be rebuilt at runtime, so the caller must not report new certificates.
     * @throws SSLException If the context could not be built.
     */
    @Nullable private static SSLContext rebuild(Factory<SSLContext> factory, @Nullable SSLContext cur, boolean apply)
        throws SSLException {
        SSLContext ctx;

        if (factory instanceof AbstractSslContextFactory) {
            AbstractSslContextFactory sslCtxFactory = (AbstractSslContextFactory)factory;

            ctx = apply ? sslCtxFactory.reload() : sslCtxFactory.build();
        }
        else
            ctx = factory.create();

        return ctx == cur ? null : ctx;
    }
}
