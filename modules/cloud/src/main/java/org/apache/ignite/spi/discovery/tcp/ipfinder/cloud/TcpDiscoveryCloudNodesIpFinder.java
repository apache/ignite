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

package org.apache.ignite.spi.discovery.tcp.ipfinder.cloud;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.sshj.config.SshjSshClientModule;
import org.jclouds.logging.log4j.config.*;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * IP finder for automatic lookup of nodes running in a cloud.
 * <p>
 * Implementation is based on Apache jclouds multi-cloud toolkit.
 * For information about jclouds visit <a href="https://jclouds.apache.org/">jclouds.apache.org</a>.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * <ul>
 *      <li>Cloud provider (see {@link #setProvider(String)})</li>
 *      <li>Identity (see {@link #setIdentity(String)})</li>
 * </ul>
 * <h2 class="header">Optional</h2>
 * <ul>
 *      <li>Credential (see {@link #setCredential(String)})</li>
 *      <li>Discovery port (see {@link #setDiscoveryPort(Integer)})</li>
 * </ul>
 * </p>
 * <p>
 * The finder forms nodes addresses, that possibly running Ignite, by getting private and public IPs of all
 * VMs in a cloud and adding {@link #discoveryPort} to them.
 * Both {@link #registerAddresses(Collection)} and {@link #unregisterAddresses(Collection)} has no effect.
 * </p>
 * <p>
 * Note, this finder is only workable when it used directly by a cloud VM.
 * Choose another implementation of {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder} for local
 * or home network tests.
 * </p>
 */
public class TcpDiscoveryCloudNodesIpFinder extends TcpDiscoveryIpFinderAdapter {
    /* Cloud provider. */
    private String provider;

    /* Cloud specific identity (user name, email address, etc.). */
    private String identity;

    /* Cloud specific credential (password, secrets key, etc.). */
    @GridToStringExclude
    private String credential;

    /* Port to use to connect to nodes across a cluster. */
    private Integer discoveryPort;

    /** Init guard. */
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init latch. */
    @GridToStringExclude
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /* JCloud compute service. */
    private ComputeService computeService;

    /**
     * Constructor.
     */
    public TcpDiscoveryCloudNodesIpFinder() {
        setShared(true);
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        initComputeService();

        Collection<InetSocketAddress> addresses = new LinkedList<>();

        try {
            for (ComputeMetadata node : computeService.listNodes()) {
                NodeMetadata metadata = computeService.getNodeMetadata(node.getId());

                for (String addr : metadata.getPrivateAddresses())
                    addresses.add(new InetSocketAddress(addr, discoveryPort));

                for (String addr : metadata.getPublicAddresses())
                    addresses.add(new InetSocketAddress(addr, discoveryPort));
            }
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to get registered addresses for the provider: " + provider);
        }

        return addresses;
    }

    /** {@inheritDoc} */
    @Override public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        // No-op
    }

    /**
     * Sets the cloud provider to use.
     *
     * <a href="https://jclouds.apache.org/reference/providers/#compute">Apache jclouds providers list</a> from
     * ComputeService section contains names of all supported providers.
     *
     * @param provider Provider name.
     */
    @IgniteSpiConfiguration(optional = false)
    public void setProvider(String provider) {
        this.provider = provider;
    }

    /**
     * Sets the identity that is used as a user name during a connection to the cloud.
     * Depending on a cloud platform it can be an email address, user name, etc.
     *
     * Refer to <a href="http://jclouds.apache.org/guides/">Apache jclouds guide</a> to get concrete information on
     * what is used as an identity for a particular cloud platform.
     *
     * @param identity Identity to use during authentication on the cloud.
     */
    @IgniteSpiConfiguration(optional = false)
    public void setIdentity(String identity) {
        this.identity = identity;
    }

    /**
     * Sets credential that is used during authentication on the cloud.
     * Depending on a cloud platform it can be a password, path to a secrets file, etc.
     *
     * Refer to <a href="http://jclouds.apache.org/guides/">Apache jclouds guide</a> to get concrete information on
     * what is used as an credential for a particular cloud platform.
     *
     * @param credential Credential to use during authentication on the cloud.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setCredential(String credential) {
        this.credential = credential;
    }

    /**
     * Sets the port that is used to discover other nodes running Apache Ignite in the cloud.
     * If doesn't set, a default port number is used.
     *
     * Note that all cloud nodes must accept connection on the same port number.
     *
     * @param discoveryPort Port number to use for discovering of other nodes.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setDiscoveryPort(Integer discoveryPort) {
        this.discoveryPort = discoveryPort;
    }

    /**
     * Initializes Apache jclouds compute service.
     */
    private void initComputeService() {
        if (initGuard.compareAndSet(false, true))
            try {
                if (provider == null)
                    throw new IgniteSpiException("Cloud provider is not set.");

                if (identity == null)
                    throw new IgniteSpiException("Cloud identity is not set.");

                try {
                    ContextBuilder ctxBuilder = ContextBuilder.newBuilder(provider);
                    ctxBuilder.credentials(identity, credential);
                    ctxBuilder.modules(ImmutableSet.<Module>of(new Log4JLoggingModule(), new SshjSshClientModule()));

                    computeService = ctxBuilder.buildView(ComputeServiceContext.class).getComputeService();
                }
                catch (Exception e) {
                    throw new IgniteSpiException("Failed to connect to the provider: " + provider, e);
                }

                if (discoveryPort == null)
                    discoveryPort = TcpDiscoverySpi.DFLT_PORT;

            } finally {
                initLatch.countDown();
            }
        else
        {
            try {
                U.await(initLatch);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteSpiException("Thread has been interrupted.", e);
            }

            if (computeService == null)
                throw new IgniteSpiException("Ip finder has not been initialized properly.");
        }
    }
}
