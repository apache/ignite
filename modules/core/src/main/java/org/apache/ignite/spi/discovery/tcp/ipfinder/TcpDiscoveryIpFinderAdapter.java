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

package org.apache.ignite.spi.discovery.tcp.ipfinder;

import java.net.InetSocketAddress;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;

/**
 * IP finder interface implementation adapter.
 */
public abstract class TcpDiscoveryIpFinderAdapter implements TcpDiscoveryIpFinder {
    /** Shared flag. */
    private boolean shared;

    /** SPI context. */
    @GridToStringExclude
    private volatile IgniteSpiContext spiCtx;

    /** Ignite instance . */
    @IgniteInstanceResource
    @GridToStringExclude
    protected Ignite ignite;

    /** {@inheritDoc} */
    @Override public void onSpiContextInitialized(IgniteSpiContext spiCtx) throws IgniteSpiException {
        this.spiCtx = spiCtx;
    }

    /** {@inheritDoc} */
    @Override public void onSpiContextDestroyed() {
        spiCtx = null;
    }

    /** {@inheritDoc} */
    @Override public void initializeLocalAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        if (!F.isEmpty(addrs))
            registerAddresses(addrs);
    }

    /** {@inheritDoc} */
    @Override public boolean isShared() {
        return shared;
    }

    /**
     * Sets shared flag. If {@code true} then it is expected that IP addresses registered
     * with IP finder will be seen by IP finders on all other nodes.
     *
     * @param shared {@code true} if this IP finder is shared.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoveryIpFinderAdapter setShared(boolean shared) {
        this.shared = shared;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryIpFinderAdapter.class, this);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }

    /**
     * @return SPI context.
     */
    protected IgniteSpiContext spiContext() {
        return spiCtx;
    }
}