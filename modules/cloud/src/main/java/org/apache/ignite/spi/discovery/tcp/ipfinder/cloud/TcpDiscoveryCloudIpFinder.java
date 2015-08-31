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

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.Location;
import org.jclouds.googlecloud.GoogleCredentialsFromJson;
import org.jclouds.location.reference.LocationConstants;

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
 *      <li>Credential path (see {@link #setCredentialPath(String)}</li>
 *      <li>Regions (see {@link #setRegions(Collection)})</li>
 *      <li>Zones (see {@link #setZones(Collection)}</li>
 * </ul>
 * </p>
 * <p>
 * The finder forms nodes addresses, that possibly running Ignite, by getting private and public IPs of all
 * VMs in a cloud and adding a port number to them.
 * The port is either the one that is set with {@link TcpDiscoverySpi#setLocalPort(int)} or
 * {@link TcpDiscoverySpi#DFLT_PORT}.
 * Make sure that all VMs start Ignite instances on the same port, otherwise they will not be able to discover each
 * other using this IP finder.
 * </p>
 * <p>
 * Both {@link #registerAddresses(Collection)} and {@link #unregisterAddresses(Collection)} has no effect.
 * </p>
 * <p>
 * Note, this finder is only workable when it used directly by cloud VM.
 * Choose another implementation of {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder} for local
 * or home network tests.
 * </p>
 * <h2 class="header">Java Example</h2>
 * <pre name="code" class="java">
 * String accountId = "your_account_id";
 * String accountKey = "your_account_key";
 *
 * TcpDiscoveryCloudIpFinder ipFinder = new TcpDiscoveryCloudIpFinder();
 *
 * ipFinder.setProvider("aws-ec2");
 * ipFinder.setIdentity(accountId);
 * ipFinder.setCredential(accountKey);
 * ipFinder.setRegions(Collections.<String>emptyList().add("us-east-1"));
 * ipFinder.setZones(Arrays.asList("us-east-1b", "us-east-1e"));
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * TcpDiscoveryCloudIpFinder can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.apache.ignite.configuration.IgniteConfiguration" singleton="true"&gt;
 *         ...
 *         &lt;property name="discoverySpi"&gt;
 *             &lt;bean class="org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi"&gt;
 *                 &lt;property name="ipFinder"&gt;
 *                     &lt;bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.cloud.TcpDiscoveryCloudIpFinder"/&gt;
 *                         &lt;property name="provider" value="google-compute-engine"/&gt;
 *                         &lt;property name="identity" value="your_service_account_email"/&gt;
 *                         &lt;property name="credentialPath" value="path_to_json_key"/&gt;
 *                         &lt;property name="zones"&gt;
 *                             &lt;list&gt;
 *                                 &lt;value>us-central1-a&lt/value&gt;
 *                                 &lt;value>asia-east1-a&lt/value&gt;
 *                             &lt;/list&gt;
 *                         &lt;/property&gt;
 *                     &lt;/bean&gt;
 *                 &lt;/property&gt;
 *
 *                 &lt;property name="socketTimeout" value="400"/&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 */
public class TcpDiscoveryCloudIpFinder extends TcpDiscoveryIpFinderAdapter {
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
    public TcpDiscoveryCloudIpFinder() {
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
                if (metadata.getStatus() != NodeMetadata.Status.RUNNING)
                    continue;

                for (String addr : metadata.getPrivateAddresses())
                    addresses.add(new InetSocketAddress(addr, 0));

                for (String addr : metadata.getPublicAddresses())
                    addresses.add(new InetSocketAddress(addr, 0));
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
     * This method should be used when an access key or private key is stored in a file.
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
                    credential = getCredentialFromFile();

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
            }
            finally {
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
     * Reads credential info from {@link #credentialPath} and returns in a string format.
     *
     * @return Credential in {@code String} representation.
     * @throws IgniteSpiException In case of error.
     */
    private String getCredentialFromFile() throws IgniteSpiException {
        try {
            String fileContents = Files.toString(new File(credentialPath), Charsets.UTF_8);

            if (provider.equals("google-compute-engine")) {
                Supplier<Credentials> credentialSupplier = new GoogleCredentialsFromJson(fileContents);

                return credentialSupplier.get().credential;
            }

            return fileContents;
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