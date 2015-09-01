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

package org.apache.ignite.spi.discovery.tcp.ipfinder.zk;

import com.google.common.collect.Sets;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;
import org.codehaus.jackson.map.annotate.JsonRootName;

/**
 * This TCP Discovery IP Finder uses Apache ZooKeeper (ZK) to locate peer nodes when bootstrapping in order to join
 * the cluster. It uses the Apache Curator library to interact with ZooKeeper in a simple manner. Specifically,
 * it uses the {@link ServiceDiscovery} recipe, which makes use of ephemeral nodes in ZK to register services.
 *
 * <p>
 * There are several ways to instantiate the TcpDiscoveryZookeeperIpFinder:
 * <li>
 *     <ul>By providing an instance of {@link CuratorFramework} directly, in which case no ZK Connection String
 *     is required.</ul>
 *     <ul>By providing a ZK Connection String through {@link #setZkConnectionString(String)}, and optionally
 *     a {@link RetryPolicy} through the setter. If the latter is not provided, a default
 *     {@link ExponentialBackoffRetry} policy is used, with a base sleep time of 1000ms and 10 retries.</ul>
 *     <ul>By providing a ZK Connection String through system property {@link #PROP_ZK_CONNECTION_STRING}. If this
 *     property is set, it overrides the ZK Connection String passed in as a property, but it does not override
 *     the {@link CuratorFramework} if provided.</ul>
 * </li>
 *
 * You may customise the base path for services, as well as the service name. By default {@link #BASE_PATH} and
 * {@link #SERVICE_NAME} are use respectively. You can also choose to enable or disable duplicate registrations. See
 * {@link #setAllowDuplicateRegistrations(boolean)} for more details.
 *
 * @see <a href="http://zookeeper.apache.org">Apache ZooKeeper</a>
 * @see <a href="http://curator.apache.org">Apache Curator</a>
 *
 * @author Raul Kripalani
 */
public class TcpDiscoveryZookeeperIpFinder extends TcpDiscoveryIpFinderAdapter {

    /** System property name to provide the ZK Connection String. */
    public static final String PROP_ZK_CONNECTION_STRING = "IGNITE_ZK_CONNECTION_STRING";

    /** Default base path for service registrations. */
    private static final String BASE_PATH = "/services";

    /** Default service name for service registrations. */
    private static final String SERVICE_NAME = "ignite";

    /** Default URI Spec to use with the {@link ServiceDiscoveryBuilder}. */
    private static final UriSpec URI_SPEC = new UriSpec("{address}:{port}");

    /** Init guard. */
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** The Curator framework in use, either injected or constructed by this component. */
    private CuratorFramework curator;

    /** The ZK Connection String if provided by the user. */
    private String zkConnectionString;

    /** Retry policy to use. */
    private RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);

    /** Base path to use, by default {#link #BASE_PATH}. */
    private String basePath = BASE_PATH;

    /** Service name to use, by default {#link #SERVICE_NAME}. */
    private String serviceName = SERVICE_NAME;

    /** Whether to allow or not duplicate registrations. See setter doc. */
    private boolean allowDuplicateRegistrations = false;

    /** The Service Discovery recipe. */
    private ServiceDiscovery<IgniteInstanceDetails> discovery;

    /** Map of the {#link ServiceInstance}s we have registered. */
    private Map<InetSocketAddress, ServiceInstance<IgniteInstanceDetails>> ourInstances = new ConcurrentHashMap<>();

    /** Constructor. */
    public TcpDiscoveryZookeeperIpFinder() {
        setShared(true);
    }

    /** Initializes this IP Finder by creating the appropriate Curator objects. */
    private void init() {
        if (!initGuard.compareAndSet(false, true))
            return;

        String sysPropZkConnString = System.getProperty(PROP_ZK_CONNECTION_STRING);

        if (sysPropZkConnString != null && sysPropZkConnString.trim().length() > 0)
            zkConnectionString = sysPropZkConnString;

        log.info("Initializing ZooKeeper IP Finder.");

        if (curator == null) {
            A.notNullOrEmpty(zkConnectionString, String.format("ZooKeeper URL (or system property %s) cannot be null " +
                "or empty if a CuratorFramework object is not provided explicitly", PROP_ZK_CONNECTION_STRING));
            curator = CuratorFrameworkFactory.newClient(zkConnectionString, retryPolicy);
        }

        if (curator.getState() != CuratorFrameworkState.STARTED)
            curator.start();

        discovery = ServiceDiscoveryBuilder.builder(IgniteInstanceDetails.class)
            .client(curator)
            .basePath(basePath)
            .serializer(new JsonInstanceSerializer<>(IgniteInstanceDetails.class))
            .build();
    }

    /** {@inheritDoc} */
    @Override public void onSpiContextDestroyed() {
        if (!initGuard.compareAndSet(true, false))
            return;

        log.info("Destroying ZooKeeper IP Finder.");

        super.onSpiContextDestroyed();

        if (curator != null)
            curator.close();

    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        init();

        if (log.isDebugEnabled())
            log.debug("Getting registered addresses from ZooKeeper IP Finder.");

        Collection<ServiceInstance<IgniteInstanceDetails>> serviceInstances;

        try {
            serviceInstances = discovery.queryForInstances(serviceName);
        } catch (Exception e) {
            log.warning("Error while getting registered addresses from ZooKeeper IP Finder.", e);
            return Collections.emptyList();
        }

        Set<InetSocketAddress> answer = new HashSet<>();

        for (ServiceInstance<IgniteInstanceDetails> si : serviceInstances)
            answer.add(new InetSocketAddress(si.getAddress(), si.getPort()));

        log.info("ZooKeeper IP Finder resolved addresses: " + answer);

        return answer;
    }

    /** {@inheritDoc} */
    @Override public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        init();

        log.info("Registering addresses with ZooKeeper IP Finder: " + addrs);

        Set<InetSocketAddress> registrationsToIgnore = Sets.newHashSet();
        if (!allowDuplicateRegistrations) {
            try {
                for (ServiceInstance<IgniteInstanceDetails> sd : discovery.queryForInstances(serviceName))
                    registrationsToIgnore.add(new InetSocketAddress(sd.getAddress(), sd.getPort()));
            }
            catch (Exception e) {
                log.warning("Error while finding currently registered services to avoid duplicate registrations", e);
                throw new IgniteSpiException(e);
            }
        }

        for (InetSocketAddress addr : addrs) {
            if (registrationsToIgnore.contains(addr))
                continue;

            try {
                ServiceInstance<IgniteInstanceDetails> si = ServiceInstance.<IgniteInstanceDetails>builder()
                        .name(serviceName)
                        .uriSpec(URI_SPEC)
                        .address(addr.getAddress().getHostAddress())
                        .port(addr.getPort())
                        .build();

                ourInstances.put(addr, si);

                discovery.registerService(si);

            } catch (Exception e) {
                log.warning(String.format("Error while registering an address from ZooKeeper IP Finder " +
                    "[message=%s,addresses=%s]", e.getMessage(), addr), e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {

        // if curator is not STARTED, we have nothing to unregister, because we are using ephemeral nodes,
        // which means that our addresses will only be registered in ZK as long as our connection is alive
        if (curator.getState() != CuratorFrameworkState.STARTED)
            return;

        log.info("Unregistering addresses with ZooKeeper IP Finder: " + addrs);

        for (InetSocketAddress addr : addrs) {
            ServiceInstance<IgniteInstanceDetails> si = ourInstances.get(addr);
            if (si == null) {
                log.warning("Asked to unregister address from ZooKeeper IP Finder, but no match was found in local " +
                        "instance map for: " + addrs);
                continue;
            }

            try {
                discovery.unregisterService(si);
            } catch (Exception e) {
                log.warning("Error while unregistering an address from ZooKeeper IP Finder: " + addr, e);
            }
        }
    }

    /**
     * @param curator A {@link CuratorFramework} instance to use. It can already be in <tt>STARTED</tt> state.
     */
    public void setCurator(CuratorFramework curator) {
        this.curator = curator;
    }

    /**
     * @return The ZooKeeper connection string, only if set explicitly. Else, it returns null.
     */
    public String getZkConnectionString() {
        return zkConnectionString;
    }

    /**
     * @param zkConnectionString ZooKeeper connection string in case a {@link CuratorFramework} is not being set explicitly.
     */
    public void setZkConnectionString(String zkConnectionString) {
        this.zkConnectionString = zkConnectionString;
    }

    /**
     * @return Retry policy in use if, and only if, it was set explicitly. Else, it returns null.
     */
    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    /**
     * @param retryPolicy {@link RetryPolicy} to use in case a ZK Connection String is being injected, or if
     *                    using a system property.
     */
    public void setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    /**
     * @return Base path for service registration in ZK. Default value: {@link #BASE_PATH}.
     */
    public String getBasePath() {
        return basePath;
    }

    /**
     * @param basePath Base path for service registration in ZK. If not passed, {@link #BASE_PATH} will be used.
     */
    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    /**
     * @return Service name being used, in Curator terms. See {@link #setServiceName(String)} for more information.
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * @param serviceName Service name to use, as defined by Curator's {#link ServiceDiscovery} recipe. In physical
     *                    ZK terms, it represents the node under {@link #basePath}, under which services will be
     *                    registered.
     */
    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    /**
     * * @return The value of this flag. See {@link #setAllowDuplicateRegistrations(boolean)} for more details.
     */
    public boolean isAllowDuplicateRegistrations() {
        return allowDuplicateRegistrations;
    }

    /**
     * @param allowDuplicateRegistrations Whether to register each node only once, or if duplicate registrations
     *                                    are allowed. Nodes will attempt to register themselves, plus those they
     *                                    know about. By default, duplicate registrations are not allowed, but you
     *                                    might want to set this property to <tt>true</tt> if you have multiple
     *                                    network interfaces or if you are facing troubles.
     */
    public void setAllowDuplicateRegistrations(boolean allowDuplicateRegistrations) {
        this.allowDuplicateRegistrations = allowDuplicateRegistrations;
    }

    /**
     * Empty DTO for storing service instances details. Currently acting as a placeholder because Curator requires
     * a payload type when registering and discovering nodes. May be enhanced in the future with further information
     * to assist discovery.
     *
     * @author Raul Kripalani
     */
    @JsonRootName("ignite_instance_details")
    private class IgniteInstanceDetails {

    }

}