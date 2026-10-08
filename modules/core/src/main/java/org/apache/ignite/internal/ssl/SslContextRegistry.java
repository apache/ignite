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

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.ssl.AbstractSslContextFactory;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/** SSL contexts of a node: one provider per configured factory, with the metrics of every transport and the automatic renewals. */
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
    public static final String HTTP_REST = "HTTP REST";

    /** */
    private final Collection<SslContextProvider> providers = new CopyOnWriteArrayList<>();

    /** */
    private final SslRenewal renewal;

    /** @param ctx Kernal context. */
    public SslContextRegistry(GridKernalContext ctx) {
        super(ctx);

        renewal = new SslRenewal(ctx.igniteInstanceName(), ctx.log(SslRenewal.class));
    }

    /**
     * @param factory Factory the transport is configured with.
     * @param transport Transport, one of the names above.
     * @return Context that passes every call to the one in use at that moment. Transports configured with the same factory share it,
     *      so that a reload cannot leave them on certificates read at different moments.
     */
    public synchronized SSLContext register(Factory<SSLContext> factory, String transport) {
        SslContextProvider provider = providers.stream().filter(p -> p.factory() == factory).findFirst().orElse(null);

        if (provider == null) {
            provider = new SslContextProvider(factory);

            provider.addTransport(transport);

            providers.add(provider);

            if (factory instanceof AbstractSslContextFactory f && f.isRenewalEnabled())
                renewal.start(provider, f.getRenewBeforeFraction());
        }
        else
            provider.addTransport(transport);

        registerMetrics(transport, provider);

        return new CurrentSslContext(provider::context);
    }

    /** @return Providers in the order they were registered. */
    public Collection<SslContextProvider> providers() {
        return Collections.unmodifiableCollection(providers);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        renewal.stop();
    }

    /**
     * @param transport Transport.
     * @param provider Provider serving it.
     */
    private void registerMetrics(String transport, SslContextProvider provider) {
        MetricRegistryImpl reg = ctx.metric().registry(metricName("ssl", transport.replace(' ', '.').toLowerCase()));

        reg.register("CertificateSubject", () -> Optional.ofNullable(provider.servedCertificate())
            .map(c -> c.getSubjectX500Principal().toString()).orElse(null), String.class,
            "Subject DN of the certificate presented on new connections.");

        reg.register("CertificateIssuer", () -> Optional.ofNullable(provider.servedCertificate())
            .map(c -> c.getIssuerX500Principal().toString()).orElse(null), String.class,
            "Issuer DN of the certificate presented on new connections.");

        reg.register("ChainNotAfter", () -> Optional.ofNullable(provider.servedChain()).map(SslCertificates::chainNotAfter).orElse(0L),
            "Earliest expiry time in the chain presented on new connections, in milliseconds; 0 if unknown.");

        reg.register("LastReloadSuccessTime", provider::lastSuccessTime, "Time of the last successful reload, in milliseconds; 0 if none.");

        reg.register("LastReloadFailureReason", provider::lastFailureReason, String.class,
            "Reason of the last failed reload since the last success.");

        reg.register("ConsecutiveReloadFailures", provider::failures, "Failed reloads in a row since the last successful one.");

        reg.register("NextRenewalTime", () -> provider.nextRenewalTime(),
            "Time of the next automatic renewal, in milliseconds; 0 if none.");
    }
}
