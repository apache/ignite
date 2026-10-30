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

package org.apache.ignite.ssl;

import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

/**
 * An initialized SSL context and a snapshot of the certificate chains loaded with it. The chains describe its key material,
 * not the certificate selected for a particular connection. Do not reinitialize the context after creating the bundle.
 */
public final class SslBundle {
    /** */
    private final SSLContext ctx;

    /** */
    private final Map<String, List<X509Certificate>> chains;

    /**
     * @param ctx Initialized SSL context.
     * @param chains Nonempty X.509 chains of key entries, by alias, own certificate first. {@code null} means certificate
     *      metadata is unavailable; an empty map means there are no chains. Trusted certificate entries and secret keys
     *      must not be included.
     * @throws IllegalArgumentException If a chain is empty.
     */
    public SslBundle(SSLContext ctx, @Nullable Map<String, List<X509Certificate>> chains) {
        A.notNull(ctx, "ctx");

        this.ctx = ctx;

        if (chains == null)
            this.chains = null;
        else {
            Map<String, List<X509Certificate>> copy = new TreeMap<>();

            chains.forEach((alias, chain) -> {
                A.ensure(!chain.isEmpty(), "Certificate chain must not be empty [alias=" + alias + ']');

                copy.put(alias, List.copyOf(chain));
            });

            this.chains = Collections.unmodifiableMap(copy);
        }
    }

    /** @return Initialized context. */
    public SSLContext context() {
        return ctx;
    }

    /**
     * @return Immutable, nonempty chains ordered by alias, or {@code null} if certificate metadata is unavailable.
     *      An empty map means there are no chains.
     */
    public @Nullable Map<String, List<X509Certificate>> certificateChains() {
        return chains;
    }
}
