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
package org.apache.ignite.snippets;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.cloud.TcpDiscoveryCloudIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.elb.TcpDiscoveryElbIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.gce.TcpDiscoveryGoogleStorageIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinder;

public class DiscoveryInTheCloud {

    public static void apacheJcloudsExample() {
        //tag::jclouds[]
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        TcpDiscoveryCloudIpFinder ipFinder = new TcpDiscoveryCloudIpFinder();

        // Configuration for AWS EC2.
        ipFinder.setProvider("aws-ec2");
        ipFinder.setIdentity("yourAccountId");
        ipFinder.setCredential("yourAccountKey");
        ipFinder.setRegions(Collections.singletonList("us-east-1"));
        ipFinder.setZones(Arrays.asList("us-east-1b", "us-east-1e"));

        spi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override default discovery SPI.
        cfg.setDiscoverySpi(spi);

        // Start a node.
        Ignition.start(cfg);
        //end::jclouds[]
    }

    public static void awsExample1() {
        //tag::aws1[]
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        BasicAWSCredentials creds = new BasicAWSCredentials("yourAccessKey", "yourSecreteKey");

        TcpDiscoveryS3IpFinder ipFinder = new TcpDiscoveryS3IpFinder();
        ipFinder.setAwsCredentials(creds);
        ipFinder.setBucketName("yourBucketName");

        spi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override default discovery SPI.
        cfg.setDiscoverySpi(spi);

        // Start a node.
        Ignition.start(cfg);
        //end::aws1[]
    }

    public static void awsExample2() {
        //tag::aws2[]
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        AWSCredentialsProvider instanceProfileCreds = new InstanceProfileCredentialsProvider(false);

        TcpDiscoveryS3IpFinder ipFinder = new TcpDiscoveryS3IpFinder();
        ipFinder.setAwsCredentialsProvider(instanceProfileCreds);
        ipFinder.setBucketName("yourBucketName");

        spi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override default discovery SPI.
        cfg.setDiscoverySpi(spi);

        // Start a node.
        Ignition.start(cfg);
        //end::aws2[]
    }

    public static void awsElbExample() {
        //tag::awsElb[]
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        BasicAWSCredentials creds = new BasicAWSCredentials("yourAccessKey", "yourSecreteKey");

        TcpDiscoveryElbIpFinder ipFinder = new TcpDiscoveryElbIpFinder();
        ipFinder.setRegion("yourElbRegion");
        ipFinder.setLoadBalancerName("yourLoadBalancerName");
        ipFinder.setCredentialsProvider(new AWSStaticCredentialsProvider(creds));

        spi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override default discovery SPI.
        cfg.setDiscoverySpi(spi);

        // Start the node.
        Ignition.start(cfg);
        //end::awsElb[]
    }

    public static void googleCloudStorageExample() {
        //tag::google[]
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        TcpDiscoveryGoogleStorageIpFinder ipFinder = new TcpDiscoveryGoogleStorageIpFinder();

        ipFinder.setServiceAccountId("yourServiceAccountId");
        ipFinder.setServiceAccountP12FilePath("pathToYourP12Key");
        ipFinder.setProjectName("yourGoogleClourPlatformProjectName");

        // Bucket name must be unique across the whole Google Cloud Platform.
        ipFinder.setBucketName("your_bucket_name");

        spi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override default discovery SPI.
        cfg.setDiscoverySpi(spi);

        // Start the node.
        Ignition.start(cfg);
        //end::google[]
    }

    public static void azureBlobStorageExample() {
        //tag::azureBlobStorage[]
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        TcpDiscoveryAzureBlobStorageIpFinder ipFinder = new TcpDiscoveryGoogleStorageIpFinder();

        finder.setAccountName("yourAccountName");
        finder.setAccountKey("yourAccountKey");
        finder.setAccountEndpoint("yourEndpoint");

        finder.setContainerName("yourContainerName");

        spi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override default discovery SPI.
        cfg.setDiscoverySpi(spi);

        // Start the node.
        Ignition.start(cfg);
        //end::azureBlobStorage[]
    }
}
