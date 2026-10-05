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
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.ssl.RenewableSslContextFactory;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/** SSL contexts of a node: one provider per configured factory, with the metrics of every transport and the automatic renewals. */
public class SslContextRegistry {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final Map<Factory<SSLContext>, SslContextProvider> providers = new IdentityHashMap<>();

    /** Everything whose certificates the commands reload and report, read from the management pool. */
    private final Collection<SslContextReloadable> reloadables = new CopyOnWriteArrayList<>();

    /** */
    private final SslRenewal renewal;

    /** @param ctx Kernal context. */
    public SslContextRegistry(GridKernalContext ctx) {
        this.ctx = ctx;

        renewal = new SslRenewal(ctx.igniteInstanceName(), ctx.log(SslRenewal.class));
    }

    /**
     * @param factory Factory the transport is configured with.
     * @param transport Transport, one of the names in {@link SslContextReloadable}.
     * @return Provider of the context the factory builds; transports configured with the same factory share it, so that a reload cannot
     *      leave them on certificates read at different moments.
     */
    public synchronized SslContextProvider provider(Factory<SSLContext> factory, String transport) {
        SslContextProvider provider = providers.get(factory);

        if (provider == null) {
            SslRenewal.Settings settings = factory instanceof RenewableSslContextFactory
                ? new SslRenewal.Settings((RenewableSslContextFactory)factory) : null;

            provider = new SslContextProvider(factory);

            provider.addTransport(transport);

            providers.put(factory, provider);
            reloadables.add(provider);

            if (settings != null)
                renewal.start(provider, settings);
        }
        else
            provider.addTransport(transport);

        registerMetrics(transport, provider);

        return provider;
    }

    /** @param comp Component that reloads a context it does not take from a provider. */
    public void register(SslContextReloadable comp) {
        reloadables.add(comp);

        for (String transport : comp.transports())
            registerMetrics(transport, comp);
    }

    /** @return Everything whose certificates the commands reload and report. */
    public Collection<SslContextReloadable> reloadables() {
        return reloadables;
    }

    /** Stops the automatic renewals. */
    public void stop() {
        renewal.stop();
    }

    /**
     * @param transport Transport, one of the names in {@link SslContextReloadable}.
     * @return Name of its metric registry.
     */
    public static String metricRegistryName(String transport) {
        return metricName("ssl", transport.replace(' ', '.').toLowerCase());
    }

    /**
     * @param transport Transport.
     * @param comp Component serving it.
     */
    private void registerMetrics(String transport, SslContextReloadable comp) {
        MetricRegistryImpl reg = ctx.metric().registry(metricRegistryName(transport));

        reg.register("CertificateSubject", () -> {
            X509Certificate cert = comp.servedCertificate();

            return cert == null ? null : cert.getSubjectX500Principal().toString();
        }, String.class, "Subject DN of the certificate presented on new connections.");

        reg.register("CertificateIssuer", () -> {
            X509Certificate cert = comp.servedCertificate();

            return cert == null ? null : cert.getIssuerX500Principal().toString();
        }, String.class, "Issuer DN of the certificate presented on new connections.");

        reg.register("CertificateNotAfter", () -> {
            X509Certificate[] chain = comp.servedChain();

            return chain == null ? 0 : SslCertificates.chainNotAfter(chain);
        }, "Earliest expiry time in the chain presented on new connections, in milliseconds; 0 if unknown.");

        reg.register("LastReloadTime", () -> comp.lastSuccessTime(), "Time of the last successful reload, in milliseconds; 0 if none.");
        reg.register("LastReloadFailure", comp::lastFailure, String.class, "Reason of the last failed reload since the last success.");
        reg.register("ReloadFailures", () -> comp.failures(), "Failed reloads in a row since the last successful one.");
        reg.register("NextRenewalTime", () -> comp.nextRenewalTime(), "Time of the next automatic renewal, in milliseconds; 0 if none.");
    }
}
