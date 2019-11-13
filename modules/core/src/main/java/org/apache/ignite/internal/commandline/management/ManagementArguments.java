/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.commandline.management;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * This class contains all possible arguments after parsing management command input.
 */
public class ManagementArguments {
    /** */
    private final ManagementCommandList subcommand;

    /** */
    private final boolean enable;

    /** */
    private List<String> srvUris;

    /** */
    private String keyStore;

    /** */
    private String keyStorePass;

    /** */
    private String trustStore;

    /** */
    private String trustStorePass;

    /** */
    private List<String> cipherSuites;

    /** */
    private long sesTimeout;

    /** */
    private long sesExpirationTimeout;

    /**
     * Creates a new instance of ManagementArguments.
     *
     * @param builder Management arguments.
     */
    public ManagementArguments(Builder builder) {
        subcommand = builder.cmd;
        enable = builder.enable;
        srvUris = builder.srvUris != null ? new ArrayList<>(builder.srvUris) : null;
        keyStore = builder.keyStore;
        keyStorePass = builder.keyStorePass;
        trustStore = builder.trustStore;
        trustStorePass = builder.trustStorePass;
        cipherSuites = builder.cipherSuites != null ? new ArrayList<>(builder.cipherSuites) : null;
        sesTimeout = builder.sesTimeout;
        sesExpirationTimeout = builder.sesExpirationTimeout;
    }

    /**
     * @return Management command.
     */
    public ManagementCommandList command() {
        return subcommand;
    }

    /**
     * @return {@code true} if strict mode enabled.
     */
    public boolean isEnable() {
        return enable;
    }

    /**
     * @return Server URI.
     */
    public List<String> getServerUris() {
        return srvUris;
    }

    /**
     * @return SSL cipher suites.
     */
    public List<String> getCipherSuites() {
        return cipherSuites;
    }

    /**
     * @return Key store.
     */
    public String getKeyStore() {
        return keyStore;
    }

    /**
     * @return Key store password.
     */
    public String getKeyStorePassword() {
        return keyStorePass;
    }

    /**
     * @return Trust store.
     */
    public String getTrustStore() {
        return trustStore;
    }

    /**
     * @return Trust store password.
     */
    public String getTrustStorePassword() {
        return trustStorePass;
    }

    /**
     * @return Session timeout.
     */
    public long getSessionTimeout() {
        return sesTimeout;
    }

    /**
     * @return Session expiration timeout.
     */
    public long getSessionExpirationTimeout() {
        return sesExpirationTimeout;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ManagementArguments.class, this);
    }

    /** */
    public static class Builder {
        /** */
        private final ManagementCommandList cmd;

        /** */
        private boolean enable;

        /** */
        private Set<String> srvUris;

        /** */
        private String keyStore;

        /** */
        private String keyStorePass;

        /** */
        private String trustStore;

        /** */
        private String trustStorePass;

        /** */
        private Set<String> cipherSuites;

        /** */
        private long sesTimeout;

        /** */
        private long sesExpirationTimeout;

        /**
         * Creates a new instance of builder.
         */
        public Builder(ManagementCommandList cmd) {
            this.cmd = cmd;
        }

        /**
         * @param enable {@code true} if should be enabled.
         * @return This instance for chaining.
         */
        public Builder setEnable(boolean enable) {
            this.enable = enable;

            return this;
        }

        /**
         * @param srvUri URI.
         * @return {@code this} for chaining.
         */
        public Builder setServerUris(Set<String> srvUri) {
            this.srvUris = srvUri;

            return this;
        }

        /**
         * @param keyStore Key store.
         * @return {@code this} for chaining.
         */
        public Builder setKeyStore(String keyStore) {
            this.keyStore = keyStore;

            return this;
        }

        /**
         * @param keyStorePass Key store password.
         * @return {@code this} for chaining.
         */
        public Builder setKeyStorePassword(String keyStorePass) {
            this.keyStorePass = keyStorePass;

            return this;
        }

        /**
         * @param trustStore Trust store.
         * @return {@code this} for chaining.
         */
        public Builder setTrustStore(String trustStore) {
            this.trustStore = trustStore;

            return this;
        }
        
        /**
         * @param trustStorePass Trust store password.
         * @return {@code this} for chaining.
         */
        public Builder setTrustStorePassword(String trustStorePass) {
            this.trustStorePass = trustStorePass;

            return this;
        }

        /**
         * @param cipherSuites SSL cipher suites.
         * @return {@code this} for chaining.
         */
        public Builder setCipherSuites(Set<String> cipherSuites) {
            this.cipherSuites = cipherSuites;

            return this;
        }

        /**
         * @param sesTimeout Session timeout.
         * @return {@code this} for chaining.
         */
        public Builder setSessionTimeout(long sesTimeout) {
            this.sesTimeout = sesTimeout;
            return this;
        }

        /**
         * @param sesExpirationTimeout Session expiration timeout.
         * @return {@code this} for chaining.
         */
        public Builder setSessionExpirationTimeout(long sesExpirationTimeout) {
            this.sesExpirationTimeout = sesExpirationTimeout;
            return this;
        }

        /**
         * @return New instance of {@link ManagementArguments} with the given parameters.
         */
        public ManagementArguments build() {
            return new ManagementArguments(this);
        }
    }
}
