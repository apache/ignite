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
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClientBuilder;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersRequest;
import com.amazonaws.services.elasticloadbalancing.model.Instance;
import com.amazonaws.services.elasticloadbalancing.model.LoadBalancerDescription;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * AWS ELB-based IP finder.
 */
public class TcpDiscoveryElbIpFinder extends TcpDiscoveryIpFinderAdapter {

    private final AmazonElasticLoadBalancing amazonELBClient;
    private final AmazonEC2 amazonEC2Client;
    private final String loadBalancerName;
    private final int port;

    public TcpDiscoveryElbIpFinder(AWSCredentialsProvider awsCredentialsProvider, String loadBalancerName, String region, int port) {
        this.loadBalancerName = loadBalancerName;
        this.port = port;
        amazonEC2Client = AmazonEC2ClientBuilder.standard()
                                .withRegion(region)
                                .withCredentials(awsCredentialsProvider)
                                .build();
        amazonELBClient =
                AmazonElasticLoadBalancingClientBuilder.standard()
                        .withRegion(region)
                        .withCredentials(awsCredentialsProvider)
                        .build();
        setShared(true);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {

        List<String>
                instanceIds =
                amazonELBClient.describeLoadBalancers(buildLoadBalancerRequest())
                        .getLoadBalancerDescriptions()
                        .stream()
                        .map(LoadBalancerDescription::getInstances)
                        .flatMap(instances -> instances.stream())
                        .map(Instance::getInstanceId)
                        .collect(toList());

        return amazonEC2Client.describeInstances(new DescribeInstancesRequest().withInstanceIds(instanceIds))
                .getReservations()
                .stream()
                .flatMap(reservation -> reservation.getInstances().stream())
                .map(instance -> new InetSocketAddress(instance.getPrivateIpAddress(), port))
                .collect(toList());
    }

    private DescribeLoadBalancersRequest buildLoadBalancerRequest() {
        return new DescribeLoadBalancersRequest().withLoadBalancerNames(loadBalancerName);
    }


   /** {@inheritDoc} */
    @Override
    public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        //nothing to do, ELB will take care of registering
    }

    /** {@inheritDoc} */
    @Override
    public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        // nothing to do, ELB will take care of un-registering
    }
}