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

package org.apache.ignite.internal.client;

import org.apache.ignite.client.IgniteClientAddressFinder;
import org.apache.ignite.client.IgniteClientConfiguration;

/**
 * Immutable client configuration.
 */
public final class IgniteClientConfigurationImpl implements IgniteClientConfiguration {
    /** Address finder. */
    private final IgniteClientAddressFinder addressFinder;

    /** Addresses. */
    private final String[] addresses;

    /** Retry limit. */
    private final int retryLimit;

    /** Connect timeout, in milliseconds. */
    private final long connectTimeout;

    /** Reconnect throttling period, in milliseconds.  */
    private final long reconnectThrottlingPeriod;

    /** Reconnect throttling retries. */
    private final int reconnectThrottlingRetries;

    /**
     * Constructor.
     *  @param addressFinder Address finder.
     * @param addresses Addresses.
     * @param retryLimit Retry limit.
     * @param connectTimeout Socket connect timeout.
     */
    public IgniteClientConfigurationImpl(
            IgniteClientAddressFinder addressFinder,
            String[] addresses,
            int retryLimit,
            long connectTimeout,
            long reconnectThrottlingPeriod,
            int reconnectThrottlingRetries
    ) {
        this.addressFinder = addressFinder;
        this.addresses = addresses;
        this.retryLimit = retryLimit;
        this.connectTimeout = connectTimeout;
        this.reconnectThrottlingPeriod = reconnectThrottlingPeriod;
        this.reconnectThrottlingRetries = reconnectThrottlingRetries;
    }

    /** {@inheritDoc} */
    @Override public IgniteClientAddressFinder addressesFinder() {
        return addressFinder;
    }

    /** {@inheritDoc} */
    @Override public String[] addresses() {
        return addresses == null ? null : addresses.clone();
    }

    /** {@inheritDoc} */
    @Override public int retryLimit() {
        return retryLimit;
    }

    /** {@inheritDoc} */
    @Override public long connectTimeout() {
        return connectTimeout;
    }

    /** {@inheritDoc} */
    @Override public long reconnectThrottlingPeriod() {
        return reconnectThrottlingPeriod;
    }

    /** {@inheritDoc} */
    @Override public int reconnectThrottlingRetries() {
        return reconnectThrottlingRetries;
    }
}
