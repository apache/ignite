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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.ssl.AbstractSslContextFactory.NODE_ACCESS;

/** Tracks the TLS factories used by a node's transports. */
public final class SslContextRegistry {
    /** */
    public static final String COMMUNICATION = "communication";

    /** */
    public static final String DISCOVERY = "discovery";

    /** */
    public static final String CLIENT_CONNECTOR = "client connector";

    /** */
    public static final String BINARY_REST = "binary REST";

    /** Entries in registration order. */
    private final List<Entry> entries = new ArrayList<>();

    /** Node logger. */
    @Nullable private final IgniteLogger log;

    /** @param log Node logger. */
    public SslContextRegistry(@Nullable IgniteLogger log) {
        this.log = log;
    }

    /**
     * @param factory Configured factory.
     * @param transport Transport using this factory.
     * @return Entry shared by transports using the same factory object.
     */
    public synchronized Entry register(Factory<SSLContext> factory, String transport) {
        for (Entry entry : entries) {
            if (entry.factory == factory) {
                entry.transports.add(transport);

                return entry;
            }
        }

        Entry entry = new Entry(factory);

        entry.transports.add(transport);
        entries.add(entry);

        Map<String, SslCertificateValidity> certs = entry.certificates();

        if (log != null && certs != null) {
            if (certs.isEmpty())
                log.warning("The TLS key store has no X.509 certificate chains [transport=" + transport + ']');

            certs.forEach((alias, validity) -> {
                if (!validity.validAt(System.currentTimeMillis()))
                    log.warning("The TLS certificate chain is not valid now [transport=" + transport + ", alias=" + alias + ']');
            });
        }

        return entry;
    }

    /**
     * @param entry Factory entry.
     * @param transport Transport that could not start.
     */
    public synchronized void unregister(Entry entry, String transport) {
        entry.transports.remove(transport);

        if (entry.transports.isEmpty())
            entries.remove(entry);
    }

    /** @return Snapshot of the registered entries. */
    public synchronized List<Entry> entries() {
        return List.copyOf(entries);
    }

    /** Current TLS context for the transports using a factory. */
    public static final class Entry {
        /** */
        private final Factory<SSLContext> factory;

        /** Base factory when its own cache supplies the configured context. */
        @Nullable private final AbstractSslContextFactory managed;

        /** Current context for a factory that supplies contexts through create() only. */
        @Nullable private volatile SSLContext ctx;

        /** */
        private final Set<String> transports = new ConcurrentSkipListSet<>();

        /** @param factory Configured factory. */
        private Entry(Factory<SSLContext> factory) {
            this.factory = factory;

            SSLContext init = factory.create();

            try {
                SslFactoryAccess.checkContext(init);
            }
            catch (SSLException e) {
                throw new IgniteException(e);
            }

            if (factory instanceof AbstractSslContextFactory base && inheritsCreate(base) && NODE_ACCESS.context(base) == init)
                managed = base;
            else {
                managed = null;
                ctx = init;
            }
        }

        /** @return Whether the factory relies on the base implementation of {@code create()}. */
        private static boolean inheritsCreate(AbstractSslContextFactory factory) {
            try {
                return factory.getClass().getMethod("create").getDeclaringClass() == AbstractSslContextFactory.class;
            }
            catch (NoSuchMethodException e) {
                throw new AssertionError(e);
            }
        }

        /** @return Context to use for a new connection. */
        public SSLContext context() {
            if (managed == null)
                return ctx;

            SSLContext cur = NODE_ACCESS.context(managed);

            return cur == null ? managed.create() : cur;
        }

        /** @return Names of the transports sharing the context. */
        public String transports() {
            return String.join(", ", transports);
        }

        /** @return Certificate validity by alias, or {@code null} if unavailable. */
        public @Nullable Map<String, SslCertificateValidity> certificates() {
            return managed == null ? null : NODE_ACCESS.certificates(managed);
        }

        /** @throws SSLException If the factory cannot provide a usable replacement. */
        public synchronized void reload() throws SSLException {
            if (managed != null) {
                NODE_ACCESS.reload(managed);

                return;
            }

            SSLContext replacement = factory.create();

            if (replacement == ctx)
                throw new SSLException("SSL context factory returned the context already in use");

            SslFactoryAccess.checkContext(replacement);

            ctx = replacement;
        }
    }
}
