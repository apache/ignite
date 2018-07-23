package org.apache.ignite.ssl;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

class SSLContextWrapper extends SSLContext {
    SSLContextWrapper(SSLContext delegate, SSLParameters sslParameters) {
        super(new DelegatingSSLContextSpi(delegate, sslParameters),
            delegate.getProvider(),
            delegate.getProtocol());
    }
}
