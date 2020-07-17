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
}
