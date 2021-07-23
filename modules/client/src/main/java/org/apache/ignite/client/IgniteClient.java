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

package org.apache.ignite.client;

import java.util.concurrent.CompletableFuture;

import org.apache.ignite.app.Ignite;
import org.apache.ignite.internal.client.TcpIgniteClient;

/**
 * Ignite client entry point.
 */
public class IgniteClient {
    /**
     * Gets a new client builder.
     *
     * @return New client builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Client builder. */
    public static class Builder {
        /** Addresses. */
        private String[] addresses;

        /**
         * Builds the client.
         *
         * @return Ignite client.
         */
        public Ignite build() {
            // TODO: Validate values IGNITE-15164.
            return buildAsync().join();
        }

        /**
         * Sets the addresses.
         *
         * @param addrs Addresses.
         * @return This instance
         */
        public Builder addresses(String... addrs) {
            addresses = addrs;

            return this;
        }

        /**
         * Builds the client.
         *
         * @return Ignite client.
         */
        public CompletableFuture<Ignite> buildAsync() {
            // TODO: Async connect IGNITE-15164.
            var cfg = new IgniteClientConfigurationImpl(null, addresses, 0);

            return CompletableFuture.completedFuture(new TcpIgniteClient(cfg));
        }
    }

    /**
     * Immutable configuration.
     */
    private static class IgniteClientConfigurationImpl implements IgniteClientConfiguration {
        /** Address finder. */
        private final IgniteClientAddressFinder addressFinder;

        /** Addresses. */
        private final String[] addresses;

        /** Retry limit. */
        private final int retryLimit;

        /**
         * Constructor.
         *
         * @param addressFinder Address finder.
         * @param addresses Addresses.
         * @param retryLimit Retry limit.
         */
        IgniteClientConfigurationImpl(IgniteClientAddressFinder addressFinder, String[] addresses, int retryLimit) {
            this.addressFinder = addressFinder;
            this.addresses = addresses;
            this.retryLimit = retryLimit;
        }

        /** {@inheritDoc} */
        @Override public IgniteClientAddressFinder getAddressesFinder() {
            return addressFinder;
        }

        /** {@inheritDoc} */
        @Override public String[] getAddresses() {
            // TODO: Defensive copy IGNITE-15164.
            return addresses;
        }

        /** {@inheritDoc} */
        @Override public int getRetryLimit() {
            return retryLimit;
        }
    }
}
