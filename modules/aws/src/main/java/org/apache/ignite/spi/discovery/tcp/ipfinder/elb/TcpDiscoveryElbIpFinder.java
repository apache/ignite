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
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClientBuilder;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersRequest;
import com.amazonaws.services.elasticloadbalancing.model.Instance;
import com.amazonaws.services.elasticloadbalancing.model.LoadBalancerDescription;
import java.util.ArrayList;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

import static com.amazonaws.util.StringUtils.isNullOrEmpty;

/**
 * AWS Classic load balancer based IP finder.
 *
 * <p>
 *     For information about Amazon Classic load balancers visit:
 *     <a href="https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/introduction.html">aws.amazon.com</a>.
 * </p>
 *
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * <ul>
 *     <li>{@link #setCredentialsProvider(AWSCredentialsProvider)}</li>
 *     <li>Classic load balancer name (see {@link #setLoadBalancerName(String)})</li>
 *     <li>Classic load balancer region (see {@link #setRegion(String)})</li>
 * </ul>
 *
 * <p> The finder will fetch all nodes connected under an Classic load balancer and share with its peers for cluster
 * awareness.</p>
 *
 * <p> Note that using AWS Classic load balancer service will result in charges to your AWS account.</p>
 *
 * <p>
 * Choose another implementation of {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder}
 * for local or home network tests.</p>
 *
 * <p> Note that this finder is shared.</p>
 *
 * <p> Note that this finder can only be used on AWS EC2 instances that belong on a Load Balancer based auto scaling group.</p>
 *
 * @see TcpDiscoveryAlbIpFinder
 */
public class TcpDiscoveryElbIpFinder extends TcpDiscoveryIpFinderAdapter {
    /** */
    private AmazonElasticLoadBalancing amazonELBClient;

    /** */
    private AmazonEC2 amazonEC2Client;

    /** */
    private AWSCredentialsProvider credsProvider;

    /** */
    private String region;

    /** */
    private String loadBalancerName;

    /**
     * Creates Classic load balancer IP finder instance.
     */
    public TcpDiscoveryElbIpFinder() {
        setShared(true);
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        initClients();

        List<String> instanceIds = new ArrayList<>();

        DescribeLoadBalancersRequest req = new DescribeLoadBalancersRequest().withLoadBalancerNames(loadBalancerName);

        List<LoadBalancerDescription> descs = amazonELBClient.describeLoadBalancers(req).getLoadBalancerDescriptions();

        for (LoadBalancerDescription desc : descs) {
            for (Instance instance : desc.getInstances())
                instanceIds.add(instance.getInstanceId());
        }

        DescribeInstancesRequest instReq = new DescribeInstancesRequest().withInstanceIds(instanceIds);

        List<Reservation> reservations = amazonEC2Client.describeInstances(instReq).getReservations();

        List<InetSocketAddress> addrs = new ArrayList<>();

        for (Reservation reservation : reservations) {
            List<com.amazonaws.services.ec2.model.Instance> instances = reservation.getInstances();

            for (com.amazonaws.services.ec2.model.Instance instance : instances)
                addrs.add(new InetSocketAddress(instance.getPrivateIpAddress(), 0));
        }

        return addrs;
    }

    /**
     * Initializing the IP finder.
     */
    private void initClients() {
        if (credsProvider == null || isNullOrEmpty(loadBalancerName) || isNullOrEmpty(region))
            throw new IgniteSpiException("One or more configuration parameters are invalid [setCredentialsProvider=" +
                credsProvider + ", setRegion=" + region + ", setLoadBalancerName=" +
                loadBalancerName + "]");

        if (amazonEC2Client == null)
            amazonEC2Client = AmazonEC2ClientBuilder.standard().withRegion(region).withCredentials(credsProvider)
                .build();

        if (amazonELBClient == null)
            amazonELBClient = AmazonElasticLoadBalancingClientBuilder.standard().withRegion(region)
                .withCredentials(credsProvider).build();
    }

    /**
     * Sets AWS Classic load balancer name which nodes are plugged under it. For details refer to Amazon API
     * reference.
     *
     * @param loadBalancerName AWS Classic load balancer name.
     */
    @IgniteSpiConfiguration(optional = false)
    public void setLoadBalancerName(String loadBalancerName) {
        this.loadBalancerName = loadBalancerName;
    }

    /**
     * Sets Classic load balancer's region.
     *
     * For details refer to Amazon API reference.
     *
     * @param region AWS Classic load balancer region (i.e: us-east-1)
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
     * @param credsProvider AWS credentials provider.
     */
    @IgniteSpiConfiguration(optional = false)
    public void setCredentialsProvider(AWSCredentialsProvider credsProvider) {
        this.credsProvider = credsProvider;
    }

    /** {@inheritDoc} */
    @Override public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        //No-op, Classic load balancer will take care of registration.
    }

    /** {@inheritDoc} */
    @Override public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        // No-op, Classic load balancer will take care of this process.
    }
}
