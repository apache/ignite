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
package org.apache.ignite.spi.discovery.tcp.ipfinder.elb;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClientBuilder;
import java.net.InetSocketAddress;
import java.util.Collection;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;

import static com.amazonaws.util.StringUtils.isNullOrEmpty;

/**
 * AWS ELB-based IP finder.
 *
 * <p>For information about Amazon ELB visit <a href="http://aws.amazon.com">aws.amazon.com</a>.</p>
 *
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * <ul>
 *     <li>{@link #setCredentialsProvider(AWSCredentialsProvider)}</li>
 *     <li>ELB name (see {@link #setLoadBalancerName(String)})</li>
 *     <li>ELB region (see {@link #setRegion(String)})</li>
 * </ul>
 *
 * <p> The finder will fetch all nodes connected under an ELB and share with its peers for cluster awareness.</p>
 *
 * <p> Note that using AWS ELB service will result in charges to your AWS account.</p>
 *
 * <p>
 * Choose another implementation of {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder}
 * for local or home network tests.</p>
 *
 * <p> Note that this finder is shared.</p>
 */
public class TcpDiscoveryElbIpFinder extends TcpDiscoveryIpFinderAdapter {
    /** */
    private AmazonElasticLoadBalancing amazonELBClient;

    /** */
    private AmazonEC2 amazonEC2Client;

    /** */
    private AWSCredentialsProvider credentialsProvider;

    /** */
    private String region;

    /** */
    private String loadBalancerName;

    /**
     * Creates ELB IP finder instance.
     */
    public TcpDiscoveryElbIpFinder() {
        setShared(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        initClients();

        // TODO ZK
        return null;
    }

    /**
     * Initializing the IP finder.
     */
    private void initClients() {
        if (credentialsProvider == null || isNullOrEmpty(loadBalancerName) || isNullOrEmpty(region))
            throw new IgniteSpiException("One or more configuration parameters are invalid [setCredentialsProvider=" +
                credentialsProvider + ", setRegion=" + region + ", setLoadBalancerName=" +
                loadBalancerName + "]");

        if (amazonEC2Client == null)
            amazonEC2Client = AmazonEC2ClientBuilder.standard().withRegion(region).withCredentials(credentialsProvider)
                .build();

        if (amazonELBClient == null)
            amazonELBClient = AmazonElasticLoadBalancingClientBuilder.standard().withRegion(region)
                .withCredentials(credentialsProvider).build();
    }

    /**
     * Sets AWS Elastic Load Balancing name which nodes are plugged under it. For details refer to Amazon API
     * reference.
     *
     * @param loadBalancerName AWS Elastic Load Balancing name.
     */
    @IgniteSpiConfiguration(optional = false)
    public void setLoadBalancerName(String loadBalancerName) {
        this.loadBalancerName = loadBalancerName;
    }

    /**
     * Sets AWS Elastic Load Balancer's region.
     *
     * For details refer to Amazon API reference.
     *
     * @param region AWS Elastic Load Balancer region (i.e: us-east-1)
     */
    @IgniteSpiConfiguration(optional = false)
    public void setRegion(String region) {
        this.region = region;
    }

    /**
     * Sets AWS credentials provider.
     *
     * For details refer to Amazon API reference.
     *
     * @param credentialsProvider AWS credentials provider.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = false)
    public void setCredentialsProvider(AWSCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        //No-op, ELB will take care of registration.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        // No-op, ELB will take care of this process.
    }
}