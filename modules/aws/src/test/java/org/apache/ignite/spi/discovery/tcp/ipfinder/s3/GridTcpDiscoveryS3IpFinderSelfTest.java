/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.discovery.tcp.ipfinder.s3;

import com.amazonaws.auth.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testsuites.bamboo.*;

import java.net.*;
import java.util.*;

/**
 * GridTcpDiscoveryS3IpFinder test.
 */
public class GridTcpDiscoveryS3IpFinderSelfTest
    extends GridTcpDiscoveryIpFinderAbstractSelfTest<GridTcpDiscoveryS3IpFinder> {
    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public GridTcpDiscoveryS3IpFinderSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected GridTcpDiscoveryS3IpFinder ipFinder() throws Exception {
        GridTcpDiscoveryS3IpFinder finder = new GridTcpDiscoveryS3IpFinder();

        injectLogger(finder);

        assert finder.isShared() : "Ip finder should be shared by default.";

        finder.setAwsCredentials(new BasicAWSCredentials(GridS3TestSuite.getAccessKey(),
            GridS3TestSuite.getSecretKey()));

        // Bucket name should be unique for the host to parallel test run on one bucket.
        finder.setBucketName("ip-finder-test-bucket-" + InetAddress.getLocalHost().getAddress()[3]);

        for (int i = 0; i < 5; i++) {
            Collection<InetSocketAddress> addrs = finder.getRegisteredAddresses();

            if (!addrs.isEmpty())
                finder.unregisterAddresses(addrs);
            else
                return finder;

            U.sleep(1000);
        }

        if (!finder.getRegisteredAddresses().isEmpty())
            throw new Exception("Failed to initialize IP finder.");

        return finder;
    }
}
