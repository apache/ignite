/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * Shared IpFinder implementation for testing purposes.
 */
public class TestDynamicIpFinder extends TcpDiscoveryVmIpFinder {

    /** */
    private boolean isAvailable = true;

    /**
     * Ctor.
     */
    public TestDynamicIpFinder() {
        setShared(true);
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        if (isAvailable)
            return super.getRegisteredAddresses();

        throw new IgniteSpiException("Service is unavailable");
    }

    /**
     * Simulates service fail.
     */
    public void breakService() {
        isAvailable = false;
    }
}
