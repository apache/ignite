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

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.io.*;
import com.google.inject.*;

import org.jclouds.*;
import org.jclouds.compute.*;
import org.jclouds.compute.domain.*;
import org.jclouds.domain.*;
import org.jclouds.location.reference.LocationConstants;

import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.internal.util.typedef.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

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
    /* JCloud default connection timeout. */
    private final static String JCLOUD_CONNECTION_TIMEOUT = "10000"; //10 secs

    /* Cloud provider. */
    private String provider;

    /* Cloud specific identity (user name, email address, etc.). */
    private String identity;

    /* Cloud specific credential (password, access key, etc.). */
    @GridToStringExclude
    private String credential;

    /* Path to a cloud specific credential. */
    @GridToStringExclude
    private String credentialPath;

    /* Regions where VMs are located. */
    private TreeSet<String> regions;

    /* Zones where VMs are located. */
    private TreeSet<String> zones;

    /* Nodes filter by regions and zones. */
    private Predicate<ComputeMetadata> nodesFilter;

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
            Set<NodeMetadata> nodes;

            if (nodesFilter != null)
                nodes = (Set<NodeMetadata>)computeService.listNodesDetailsMatching(nodesFilter);
            else {
                nodes = new HashSet<>();

                for (ComputeMetadata metadata : computeService.listNodes())
                    nodes.add(computeService.getNodeMetadata(metadata.getId()));
            }

            for (NodeMetadata metadata : nodes) {
                for (String addr : metadata.getPrivateAddresses())
                    addresses.add(new InetSocketAddress(addr, discoveryPort));

                for (String addr : metadata.getPublicAddresses())
                    addresses.add(new InetSocketAddress(addr, discoveryPort));
            }
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to get registered addresses for the provider: " + provider, e);
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
     * Depending on a cloud platform it can be a password or access key.
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
     * Sets the path to a credential that is used during authentication on the cloud.
     *
     * This method should be used when an access key or private key is stored in a plain or PEM file without
     * a passphrase.
     * Content of the file, referred by @{code credentialPath}, is fully read and used as a access key or private key
     * during authentication.
     *
     * Refer to <a href="http://jclouds.apache.org/guides/">Apache jclouds guide</a> to get concrete information on
     * what is used as an credential for a particular cloud platform.
     *
     * @param credentialPath Path to the credential to use during authentication on the cloud.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setCredentialPath(String credentialPath) {
        this.credentialPath = credentialPath;
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
     * Sets list of zones where VMs are located.
     *
     * If the zones are not set then every zone from regions, set by {@link #setRegions(Collection)}}, will be
     * taken into account.
     *
     * Note, that some cloud providers, like Rackspace, doesn't have a notion of a zone. For such
     * providers a call to this method is redundant.
     *
     * @param zones Zones where VMs are located or null if to take every zone into account.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setZones(Collection<String> zones) {
        if (F.isEmpty(zones))
            return;

        this.zones = new TreeSet<>(zones);
    }

    /**
     * Sets list of regions where VMs are located.
     *
     * If the regions are not set then every region, that a cloud provider has, will be investigated. This could lead
     * to significant performance degradation.
     *
     * Note, that some cloud providers, like Google Compute Engine, doesn't have a notion of a region. For such
     * providers a call to this method is redundant.
     *
     * @param regions Regions where VMs are located or null if to check every region a provider has.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setRegions(Collection<String> regions) {
        if (F.isEmpty(regions))
            return;

        this.regions = new TreeSet<>(regions);
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

                if (credential != null && credentialPath != null)
                    throw new IgniteSpiException("Both credential and credentialPath are set. Use only one method.");

                if (credentialPath != null)
                    credential = getPrivateKeyFromFile();

                try {
                    ContextBuilder ctxBuilder = ContextBuilder.newBuilder(provider);

                    ctxBuilder.credentials(identity, credential);

                    Properties properties = new Properties();
                    properties.setProperty(Constants.PROPERTY_SO_TIMEOUT, JCLOUD_CONNECTION_TIMEOUT);
                    properties.setProperty(Constants.PROPERTY_CONNECTION_TIMEOUT, JCLOUD_CONNECTION_TIMEOUT);

                    if (!F.isEmpty(regions))
                        properties.setProperty(LocationConstants.PROPERTY_REGIONS, keysSetToStr(regions));

                    if (!F.isEmpty(zones))
                        properties.setProperty(LocationConstants.PROPERTY_ZONES, keysSetToStr(zones));

                    ctxBuilder.overrides(properties);

                    computeService = ctxBuilder.buildView(ComputeServiceContext.class).getComputeService();

                    if (!F.isEmpty(zones) || !F.isEmpty(regions)) {
                        nodesFilter = new Predicate<ComputeMetadata>() {
                            @Override public boolean apply(ComputeMetadata computeMetadata) {
                                String region = null;
                                String zone = null;

                                Location location = computeMetadata.getLocation();

                                while (location != null) {
                                    switch (location.getScope()) {
                                        case ZONE:
                                            zone = location.getId();
                                            break;

                                        case REGION:
                                            region = location.getId();
                                            break;
                                    }

                                    location = location.getParent();
                                }

                                if (regions != null && region != null && !regions.contains(region))
                                    return false;

                                if (zones != null && zone != null && !zones.contains(zone))
                                    return false;

                                return true;
                            }
                        };
                    }
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

    /**
     * Retrieves a private key from the secrets file.
     *
     * @return Private key
     */
    private String getPrivateKeyFromFile() throws IgniteSpiException {
        try {
            return Files.toString(new File(credentialPath), Charsets.UTF_8);
        }
        catch (IOException e) {
            throw new IgniteSpiException("Failed to retrieve the private key from the file: " + credentialPath, e);
        }
    }

    /**
     * Converts set keys to string.
     *
     * @param set Set.
     * @return String where keys delimited by ','.
     */
    private String keysSetToStr(Set<String> set) {
        Iterator<String> iter = set.iterator();
        StringBuilder builder = new StringBuilder();

        while (iter.hasNext()) {
            builder.append(iter.next());

            if (iter.hasNext())
                builder.append(',');
        }

        return builder.toString();
    }
}
