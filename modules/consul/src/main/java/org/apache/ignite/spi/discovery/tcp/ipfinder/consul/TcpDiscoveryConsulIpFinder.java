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
package org.apache.ignite.spi.discovery.tcp.ipfinder.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;
import org.apache.ignite.spi.discovery.tcp.ipfinder.consul.util.PublicIpService;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consul-based IP finder.
 * <p>
 * For information about Concul service visit <a href="https://www.consul.io">consul.io</a>.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * <ul>
 * <li>Consult host url</li>
 * </ul>
 * <h2 class="header">Optional</h2>
 * <ul>
 * <li>Shared flag</li>
 * <li>PublicIpMapRequired flag</li>
 * </ul>
 * <p>
 * The finder will create S3 bucket with configured name. The bucket will contain entries named
 * like the following: {@code 192.168.1.136#1001}.
 * <p>
 * Note that storing data in Concul service.
 * Choose another implementation of {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder} for local
 * or home network tests.
 * <p>
 * Note that this finder is shared by default (see {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder#isShared()}.
 */
public class TcpDiscoveryConsulIpFinder extends TcpDiscoveryIpFinderAdapter {
    /**
     * Delimiter to use in entries name.
     */
    public static final String DELIM = ".";
    /**
     * Init guard.
     */
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();
    /**
     * Init latch.
     */
    @GridToStringExclude
    private final CountDownLatch initLatch = new CountDownLatch(1);
    /**
     * Client to interact with S3 storage.
     */
    @GridToStringExclude
    ConsulClient consulClient;
    String hostUrl;
    String serviceName = "ignite";
    /**
     * Grid logger.
     */
    @LoggerResource
    private IgniteLogger log;
    @GridToStringExclude
    private boolean isPublicIpMapRequired = false;

     /**
     * Constructor.
     */
    public TcpDiscoveryConsulIpFinder() {
        setShared(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        initClient();

        Collection<InetSocketAddress> addrs = new LinkedList<>();

        try {
            Response<List<GetValue>> list = consulClient.getKVValues(this.serviceName);

            for (GetValue sum : list.getValue()) {
                String key = sum.getKey();

                StringTokenizer st = new StringTokenizer(key, DELIM);

                if (st.countTokens() != 3)
                    U.error(log, "Failed to parse S3 entry due to invalid format: " + key);
                else {
                    String serviceName = st.nextToken();
                    String addrStr = st.nextToken();
                    String portStr = st.nextToken();

                    int port = -1;

                    try {
                        port = Integer.parseInt(portStr);
                    } catch (NumberFormatException e) {
                        U.error(log, "Failed to parse port for S3 entry: " + key, e);
                    }

                    if (port != -1)
                        try {
                            addrs.add(new InetSocketAddress(addrStr, port));
                        } catch (IllegalArgumentException e) {
                            U.error(log, "Failed to parse port for S3 entry: " + key, e);
                        }
                }
            }
        } catch (Exception e) {
            throw new IgniteSpiException("Failed to list objects in the bucket: ", e);
        }

        return addrs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        assert !F.isEmpty(addrs);

        initClient();

        for (InetSocketAddress addr : addrs) {
            String key = key(addr);
            try {
                String communicationIp = addr.getAddress().getHostAddress();
                if (isPublicIpMapRequired) {
                    communicationIp = PublicIpService.getPublicIp();
                }
                consulClient.setKVValue(key, communicationIp.trim());
            } catch (Exception e) {
                throw new IgniteSpiException("Failed to put entry [entry=" + key + ']', e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        assert !F.isEmpty(addrs);

        initClient();

        for (InetSocketAddress addr : addrs) {
            String key = key(addr);

            try {
                consulClient.deleteKVValue(key);
            } catch (Exception e) {
                throw new IgniteSpiException("Failed to delete entry [" + key + ']', e);
            }
        }
    }

    /**
     * Gets key for provided address.
     *
     * @param addr Node address.
     * @return Key.
     */
    private String key(InetSocketAddress addr) {
        assert addr != null;

        SB sb = new SB();

        sb.a(this.serviceName)
                .a(DELIM).a(addr.getAddress()
                .getHostAddress())
                .a(DELIM)
                .a(addr.getPort());

        return sb.toString();
    }

    /**
     * ConsulClient initialization.
     *
     * @throws org.apache.ignite.spi.IgniteSpiException In case of error.
     */
    @SuppressWarnings({"BusyWait"})
    private void initClient() throws IgniteSpiException {
        if (initGuard.compareAndSet(false, true))
            try {
                if (hostUrl == null)
                    throw new IgniteSpiException("Consul HostURL not set.");

                if (consulClient == null)
                    U.warn(log, "Consul consulClient configuration is not set (will use default).");


                consulClient = createConsulClient();
                this.isPublicIpMapRequired = Optional.of(this.isPublicIpMapRequired).orElse(false);

            } finally {
                initLatch.countDown();
            }
        else {
            try {
                U.await(initLatch);
            } catch (IgniteInterruptedCheckedException e) {
                throw new IgniteSpiException("Thread has been interrupted.", e);
            }

            if (consulClient == null)
                throw new IgniteSpiException("Ip finder has not been initialized properly.");
        }
    }

    /**
     * Instantiates {@code ConsulClient} instance.
     *
     * @return Client instance to use to connect to Consul.
     */
    private ConsulClient createConsulClient() {
        return new ConsulClient(this.hostUrl);
    }

    /**
     * Sets hostUrl this one.
     * <p>
     * For details refer to Consul Java API reference.
     *
     * @param hostUrl AWS credentials provider.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = false)
    public TcpDiscoveryConsulIpFinder setHostUrl(String hostUrl) {
        this.hostUrl = hostUrl;
        return this;
    }

    /**
     * Sets hostUrl this one.
     * <p>
     * For details refer to Consul Java API reference.
     *
     * @param isPublicIpMapRequired get public IP
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = false)
    public TcpDiscoveryConsulIpFinder isPublicIpMapRequired(boolean isPublicIpMapRequired) {
        this.isPublicIpMapRequired = isPublicIpMapRequired;
        return this;
    }

    /**
     * Sets serviceName this one.
     * <p>
     * For details refer to Consul Java API reference.
     *
     * @param serviceName AWS credentials provider.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoveryConsulIpFinder setServiceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TcpDiscoveryConsulIpFinder setShared(boolean shared) {
        super.setShared(shared);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return S.toString(TcpDiscoveryConsulIpFinder.class, this, "super", super.toString());
    }
}
