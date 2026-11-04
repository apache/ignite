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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

/** SSL contexts of a node: one provider per configured factory, shared by its transports. */
public class SslContextRegistry extends GridProcessorAdapter {
    /** */
    public static final String COMMUNICATION = "communication";

    /** */
    public static final String DISCOVERY = "discovery";

    /** */
    public static final String CLIENT_CONNECTOR = "client connector";

    /** */
    public static final String BINARY_REST = "binary REST";

    /** */
    private final Collection<SslContextProvider> providers = new CopyOnWriteArrayList<>();

    /** @param ctx Kernal context. */
    public SslContextRegistry(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param factory Factory the transport is configured with.
     * @param transport Transport name from the constants of this class.
     * @return Context shared by the transports using this factory. New connections use its current material.
     */
    public synchronized SSLContext register(Factory<SSLContext> factory, String transport) {
        SslContextProvider provider = providers.stream().filter(p -> p.factory() == factory).findFirst().orElse(null);

        if (provider == null) {
            provider = new SslContextProvider(factory);
            provider.addTransport(transport);
            warnIfUnusable(provider);

            providers.add(provider);
        }
        else
            provider.addTransport(transport);

        return provider.sharedContext();
    }

    /**
     * Warns about key material the node starts with but reload would reject.
     *
     * @param provider Newly registered provider.
     */
    private void warnIfUnusable(SslContextProvider provider) {
        Map<String, List<X509Certificate>> chains = provider.certificateChains();

        if (chains == null)
            return;

        if (chains.isEmpty())
            U.warn(log, "The key material has no X.509 certificate chains, so the node cannot present a certificate; " +
                "reload rejects such key material");

        for (Map.Entry<String, List<X509Certificate>> entry : chains.entrySet()) {
            X509Certificate invalid = SslCertificates.invalidAt(entry.getValue(), System.currentTimeMillis());

            if (invalid != null) {
                U.warn(log, "The TLS certificate chain in use is not valid now" +
                    "; put a valid certificate in place and run control.sh --ssl reload [alias=" + entry.getKey() + ", " +
                    SslCertificates.describe(invalid) + ']');
            }
        }
    }

    /** @return One provider per factory object, in registration order; sharing a factory never adds another provider. */
    public Collection<SslContextProvider> providers() {
        return Collections.unmodifiableCollection(providers);
    }
}
