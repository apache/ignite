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
package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import java.security.SecureRandom;
import java.util.function.Supplier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLContextSpi;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

/** SSL context that passes every call to the context in use at that moment, so that Jetty serves reloaded certificates. */
class CurrentSslContext extends SSLContext {
    /** @param ctx Context in use at the moment it is asked. */
    CurrentSslContext(Supplier<SSLContext> ctx) {
        super(new Spi(ctx), ctx.get().getProvider(), ctx.get().getProtocol());
    }

    /** */
    private static class Spi extends SSLContextSpi {
        /** */
        private final Supplier<SSLContext> ctx;

        /** @param ctx Context in use at the moment it is asked. */
        private Spi(Supplier<SSLContext> ctx) {
            this.ctx = ctx;
        }

        /** {@inheritDoc} */
        @Override protected void engineInit(KeyManager[] km, TrustManager[] tm, SecureRandom rnd) {
            throw new UnsupportedOperationException("The SSL context is built by its factory");
        }

        /** {@inheritDoc} */
        @Override protected SSLSocketFactory engineGetSocketFactory() {
            return ctx.get().getSocketFactory();
        }

        /** {@inheritDoc} */
        @Override protected SSLServerSocketFactory engineGetServerSocketFactory() {
            return ctx.get().getServerSocketFactory();
        }

        /** {@inheritDoc} */
        @Override protected SSLEngine engineCreateSSLEngine() {
            return ctx.get().createSSLEngine();
        }

        /** {@inheritDoc} */
        @Override protected SSLEngine engineCreateSSLEngine(String host, int port) {
            return ctx.get().createSSLEngine(host, port);
        }

        /** {@inheritDoc} */
        @Override protected SSLSessionContext engineGetServerSessionContext() {
            return ctx.get().getServerSessionContext();
        }

        /** {@inheritDoc} */
        @Override protected SSLSessionContext engineGetClientSessionContext() {
            return ctx.get().getClientSessionContext();
        }

        /**
         * Parameters of a new engine. Jetty enables the protocols and cipher suites it finds here on every engine, while a context of
         * {@link org.apache.ignite.ssl.SslContextFactory} applies its own ones only to the engines it creates and reports the defaults
         * of the JVM.
         */
        @Override protected SSLParameters engineGetDefaultSSLParameters() {
            return ctx.get().createSSLEngine().getSSLParameters();
        }

        /** {@inheritDoc} */
        @Override protected SSLParameters engineGetSupportedSSLParameters() {
            return ctx.get().getSupportedSSLParameters();
        }
    }
}
