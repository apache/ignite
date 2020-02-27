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
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClientBuilder;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetHealthRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetDescription;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetHealthDescription;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import org.apache.http.conn.util.InetAddressUtils;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;

import static com.amazonaws.util.StringUtils.isNullOrEmpty;

/**
 * AWS Application load balancer(ALB) based IP finder.
 *
 * <p>
 *     For information about Amazon Application load balancer visit:
 *     <a href="https://docs.aws.amazon.com/elasticloadbalancing/latest/application/introduction.html">aws.amazon.com</a>.
 * </p>
 *
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * <ul>
 *     <li>{@link #setCredentialsProvider(AWSCredentialsProvider)}</li>
 *     <li>Application load balancer target group ARN name (see {@link #setTargetGrpARN(String)})</li>
 *     <li>Application load balancer region (see {@link #setRegion(String)})</li>
 * </ul>
 *
 * <p> The finder will fetch all nodes connected under an Application load balancer and share with its peers for cluster
 * awareness.</p>
 *
 * <p> Note that using AWS Application load balancer service will result in charges to your AWS account.</p>
 *
 * <p>
 * Choose another implementation of {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder} for local
 * or home network tests.</p>
 *
 * <p> Note that this finder is shared.</p>
 *
 * <p> Note that this finder can only be used on AWS EC2 instances that belong on a Load Balancer based auto scaling group.</p>
 *
 * @see TcpDiscoveryElbIpFinder
 */
public class TcpDiscoveryAlbIpFinder extends TcpDiscoveryIpFinderAdapter {
    /** */
    private AmazonElasticLoadBalancing amazonELBClient;

    /** */
    private String targetGrpARN;

    /** */
    private AmazonEC2 amazonEC2Client;

    /** */
    private AWSCredentialsProvider credsProvider;

    /** */
    private String region;

    /**
     * Creates Application load balancer IP finder instance.
     */
    public TcpDiscoveryAlbIpFinder() {
        setShared(true);
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        initClients();

        DescribeTargetHealthRequest req = new DescribeTargetHealthRequest().withTargetGroupArn(targetGrpARN);

        List<TargetHealthDescription> desc = amazonELBClient.describeTargetHealth(req).getTargetHealthDescriptions();

        // instance ips
        List<String> instanceIps = new LinkedList<>();
        // instance ids
        List<String> instanceIds = new LinkedList<>();

        // Fetch the ids of instances in the given ARN via target health
        for (TargetHealthDescription targetHealthDesc : desc) {
            TargetDescription target = targetHealthDesc.getTarget();
            String targetId = target.getId();

            // divide the target ids into ips and instance ids
            if (isIPAddress(targetId))
                instanceIps.add(targetId);
            else
                instanceIds.add(targetId);
        }

        DescribeInstancesRequest descInstReq = new DescribeInstancesRequest().withInstanceIds(instanceIds);

        List<Reservation> reservations = amazonEC2Client.describeInstances(descInstReq).getReservations();

        // Convert instance ids to instance ips
        for (Reservation reservation : reservations) {
            List<Instance> instances = reservation.getInstances();

            for (Instance instance : instances) {
                String ip = instance.getPrivateIpAddress();
                instanceIps.add(ip);
            }
        }

        List<InetSocketAddress> addrs = new LinkedList<>();

        for (String ip : instanceIps) {
            InetSocketAddress addr = new InetSocketAddress(ip, 0);
            addrs.add(addr);
        }

        return addrs;
    }

    /**
     * Checks if the given id is a valid IP address
     *
     * @param id ip to be checked.
     */
    private boolean isIPAddress(String id) {
        return InetAddressUtils.isIPv4Address(id) ||
            InetAddressUtils.isIPv4MappedIPv64Address(id) ||
            InetAddressUtils.isIPv6Address(id) ||
            InetAddressUtils.isIPv6HexCompressedAddress(id) ||
            InetAddressUtils.isIPv6StdAddress(id);
    }

    /**
     * Initializing the IP finder.
     */
    private void initClients() {
        if (credsProvider == null || isNullOrEmpty(targetGrpARN) || isNullOrEmpty(region))
            throw new IgniteSpiException("One or more configuration parameters are invalid [setCredentialsProvider=" +
                credsProvider + ", setRegion=" + region + ", setTargetGroupARN=" +
                targetGrpARN + "]");

        if (amazonEC2Client == null)
            amazonEC2Client = AmazonEC2ClientBuilder.standard().withRegion(region).withCredentials(credsProvider)
                .build();

        if (amazonELBClient == null)
            amazonELBClient = AmazonElasticLoadBalancingClientBuilder.standard().withRegion(region)
                .withCredentials(credsProvider).build();
    }

    /**
     * Sets AWS Application Load Balancer's target group ARN. For details refer to Amazon API reference.
     *
     * @param targetGrpARN Target group ARN attached to an AWS Application Load Balancer.
     */
    @IgniteSpiConfiguration(optional = false)
    public void setTargetGrpARN(String targetGrpARN) {
        this.targetGrpARN = targetGrpARN;
    }

    /**
     * Sets AWS Application Load Balancer's region.
     *
     * For details refer to Amazon API reference.
     *
     * @param region AWS Elastic Load Balancer region (e.g: us-east-1)
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
        //No-op, Application load balancer will take care of registration.
    }

    /** {@inheritDoc} */
    @Override public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        // No-op, Application load balancer will take care of this process.
    }
}
