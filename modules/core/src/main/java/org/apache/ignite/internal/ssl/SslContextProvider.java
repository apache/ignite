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

import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.ssl.SslFactoryAccess.ReloadOutcome;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.apache.ignite.ssl.SslBundle;
import org.apache.ignite.ssl.SslContextFactory;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.ssl.AbstractSslContextFactory.NODE_ACCESS;

/** Shares one TLS context among the transports configured with the same factory. */
public class SslContextProvider {
    /** Factory configured by the user, retained for registration by identity. */
    private final Factory<SSLContext> factory;

    /** Factory that owns the context, adapting a plain factory when necessary. */
    private final AbstractSslContextFactory sslFactory;

    /** Shared context that forwards new engines and socket factories to the factory's accepted material. */
    private final SSLContext sharedCtx;

    /** */
    private final Set<String> transports = new ConcurrentSkipListSet<>();

    /** @param factory Factory the transports are configured with. */
    SslContextProvider(Factory<SSLContext> factory) {
        this.factory = factory;

        sslFactory = factory instanceof AbstractSslContextFactory f ? f : new SslContextFactory() {
            /** {@inheritDoc} */
            @Override protected SslBundle createSslBundle() {
                return new SslBundle(factory.create(), null);
            }
        };

        sharedCtx = new CurrentSslContext(sslFactory.create(), sslFactory::create);
    }

    /** @return Factory the transport is configured with. */
    Factory<SSLContext> factory() {
        return factory;
    }

    /** @return Context and certificate metadata currently accepted by the factory. */
    SslBundle bundle() {
        return NODE_ACCESS.current(sslFactory);
    }

    /** @return Context shared by every transport configured with this factory. */
    SSLContext sharedContext() {
        return sharedCtx;
    }

    /** @param transport Transport the context is handed to. */
    void addTransport(String transport) {
        transports.add(transport);
    }

    /** @return Transport names in alphabetical order, separated by commas. */
    public String transports() {
        return String.join(", ", transports);
    }

    /** @return Chains by key alias, or {@code null} when the factory supplies no metadata. */
    public @Nullable Map<String, List<X509Certificate>> certificateChains() {
        return bundle().certificateChains();
    }

    /** @return Context and certificate metadata accepted by the factory's initialization. */
    public SslBundle reload() {
        return NODE_ACCESS.reload(sslFactory);
    }

    /** @return Snapshot of the last successful reload and subsequent failures. */
    public ReloadOutcome reloadOutcome() {
        return NODE_ACCESS.reloadOutcome(sslFactory);
    }
}
