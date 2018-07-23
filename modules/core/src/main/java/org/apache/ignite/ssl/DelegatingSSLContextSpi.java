package org.apache.ignite.ssl;

import java.security.KeyManagementException;
import java.security.SecureRandom;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLContextSpi;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

class DelegatingSSLContextSpi extends SSLContextSpi {

    private final SSLContext delegate;

    private final SSLParameters parameters;

    DelegatingSSLContextSpi(SSLContext delegate,
        SSLParameters parameters) {
        this.delegate = delegate;
        this.parameters = parameters;
    }

    @Override
    protected void engineInit(KeyManager[] keyManagers,
        TrustManager[] trustManagers, SecureRandom secureRandom)
        throws KeyManagementException {
        delegate.init(keyManagers, trustManagers, secureRandom);
    }

    @Override
    protected SSLSocketFactory engineGetSocketFactory() {
        return new SSLSocketFactoryWrapper(delegate.getSocketFactory(), parameters);
    }

    @Override
    protected SSLServerSocketFactory engineGetServerSocketFactory() {
        return new SSLServerSocketFactoryWrapper(delegate.getServerSocketFactory(),
            parameters);
    }

    @Override
    protected SSLEngine engineCreateSSLEngine() {
        final SSLEngine engine = delegate.createSSLEngine();
        if (parameters != null)
            engine.setSSLParameters(parameters);
        return engine;
    }

    @Override
    protected SSLEngine engineCreateSSLEngine(String s, int i) {
        final SSLEngine engine = delegate.createSSLEngine();
        if (parameters != null)
            engine.setSSLParameters(parameters);
        return engine;
    }

    @Override
    protected SSLSessionContext engineGetServerSessionContext() {
        return delegate.getServerSessionContext();
    }

    @Override
    protected SSLSessionContext engineGetClientSessionContext() {
        return delegate.getClientSessionContext();
    }

    @Override
    protected SSLParameters engineGetDefaultSSLParameters() {
        return delegate.getDefaultSSLParameters();
    }

    @Override
    protected SSLParameters engineGetSupportedSSLParameters() {
        return delegate.getSupportedSSLParameters();
    }
}
