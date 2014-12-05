/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.ipfinder.s3;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import org.apache.ignite.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * AWS S3-based IP finder.
 * <p>
 * For information about Amazon S3 visit <a href="http://aws.amazon.com">aws.amazon.com</a>.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * <ul>
 *      <li>AWS credentials (see {@link #setAwsCredentials(AWSCredentials)})</li>
 *      <li>Bucket name (see {@link #setBucketName(String)})</li>
 * </ul>
 * <h2 class="header">Optional</h2>
 * <ul>
 *      <li>Client configuration (see {@link #setClientConfiguration(ClientConfiguration)})</li>
 *      <li>Shared flag (see {@link #setShared(boolean)})</li>
 * </ul>
 * <p>
 * The finder will create S3 bucket with configured name. The bucket will contain entries named
 * like the following: {@code 192.168.1.136#1001}.
 * <p>
 * Note that storing data in AWS S3 service will result in charges to your AWS account.
 * Choose another implementation of {@link GridTcpDiscoveryIpFinder} for local
 * or home network tests.
 * <p>
 * Note that this finder is shared by default (see {@link GridTcpDiscoveryIpFinder#isShared()}.
 */
public class GridTcpDiscoveryS3IpFinder extends GridTcpDiscoveryIpFinderAdapter {
    /** Delimiter to use in S3 entries name. */
    public static final String DELIM = "#";

    /** Entry content. */
    private static final byte[] ENTRY_CONTENT = new byte[] {1};

    /** Entry metadata with content length set. */
    private static final ObjectMetadata ENTRY_METADATA;

    static {
        ENTRY_METADATA = new ObjectMetadata();

        ENTRY_METADATA.setContentLength(ENTRY_CONTENT.length);
    }

    /** Grid logger. */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** Client to interact with S3 storage. */
    @GridToStringExclude
    private AmazonS3 s3;

    /** Bucket name. */
    private String bucketName;

    /** Init guard. */
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init latch. */
    @GridToStringExclude
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** Amazon client configuration. */
    private ClientConfiguration cfg;

    /** AWS Credentials. */
    @GridToStringExclude
    private AWSCredentials cred;

    /**
     * Constructor.
     */
    public GridTcpDiscoveryS3IpFinder() {
        setShared(true);
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        initClient();

        Collection<InetSocketAddress> addrs = new LinkedList<>();

        try {
            ObjectListing list = s3.listObjects(bucketName);

            while (true) {
                for (S3ObjectSummary sum : list.getObjectSummaries()) {
                    String key = sum.getKey();

                    StringTokenizer st = new StringTokenizer(key, DELIM);

                    if (st.countTokens() != 2)
                        U.error(log, "Failed to parse S3 entry due to invalid format: " + key);
                    else {
                        String addrStr = st.nextToken();
                        String portStr = st.nextToken();

                        int port = -1;

                        try {
                            port = Integer.parseInt(portStr);
                        }
                        catch (NumberFormatException e) {
                            U.error(log, "Failed to parse port for S3 entry: " + key, e);
                        }

                        if (port != -1)
                            try {
                                addrs.add(new InetSocketAddress(addrStr, port));
                            }
                            catch (IllegalArgumentException e) {
                                U.error(log, "Failed to parse port for S3 entry: " + key, e);
                            }
                    }
                }

                if (list.isTruncated())
                    list = s3.listNextBatchOfObjects(list);
                else
                    break;
            }
        }
        catch (AmazonClientException e) {
            throw new IgniteSpiException("Failed to list objects in the bucket: " + bucketName, e);
        }

        return addrs;
    }

    /** {@inheritDoc} */
    @Override public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        assert !F.isEmpty(addrs);

        initClient();

        for (InetSocketAddress addr : addrs) {
            String key = key(addr);

            try {
                s3.putObject(bucketName, key, new ByteArrayInputStream(ENTRY_CONTENT), ENTRY_METADATA);
            }
            catch (AmazonClientException e) {
                throw new IgniteSpiException("Failed to put entry [bucketName=" + bucketName +
                    ", entry=" + key + ']', e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        assert !F.isEmpty(addrs);

        initClient();

        for (InetSocketAddress addr : addrs) {
            String key = key(addr);

            try {
                s3.deleteObject(bucketName, key);
            }
            catch (AmazonClientException e) {
                throw new IgniteSpiException("Failed to delete entry [bucketName=" + bucketName +
                    ", entry=" + key + ']', e);
            }
        }
    }

    /**
     * Gets S3 key for provided address.
     *
     * @param addr Node address.
     * @return Key.
     */
    private String key(InetSocketAddress addr) {
        assert addr != null;

        SB sb = new SB();

        sb.a(addr.getAddress().getHostAddress())
            .a(DELIM)
            .a(addr.getPort());

        return sb.toString();
    }

    /**
     * Amazon s3 client initialization.
     *
     * @throws org.gridgain.grid.spi.IgniteSpiException In case of error.
     */
    @SuppressWarnings({"BusyWait"})
    private void initClient() throws IgniteSpiException {
        if (initGuard.compareAndSet(false, true))
            try {
                if (cred == null)
                    throw new IgniteSpiException("AWS credentials are not set.");

                if (cfg == null)
                    U.warn(log, "Amazon client configuration is not set (will use default).");

                if (F.isEmpty(bucketName))
                    throw new IgniteSpiException("Bucket name is null or empty (provide bucket name and restart).");

                s3 = cfg != null ? new AmazonS3Client(cred, cfg) : new AmazonS3Client(cred);

                if (!s3.doesBucketExist(bucketName)) {
                    try {
                        s3.createBucket(bucketName);

                        if (log.isDebugEnabled())
                            log.debug("Created S3 bucket: " + bucketName);

                        while (!s3.doesBucketExist(bucketName))
                            try {
                                U.sleep(200);
                            }
                            catch (GridInterruptedException e) {
                                throw new IgniteSpiException("Thread has been interrupted.", e);
                            }
                    }
                    catch (AmazonClientException e) {
                        if (!s3.doesBucketExist(bucketName)) {
                            s3 = null;

                            throw new IgniteSpiException("Failed to create bucket: " + bucketName, e);
                        }
                    }
                }
            }
            finally {
                initLatch.countDown();
            }
        else {
            try {
                U.await(initLatch);
            }
            catch (GridInterruptedException e) {
                throw new IgniteSpiException("Thread has been interrupted.", e);
            }

            if (s3 == null)
                throw new IgniteSpiException("Ip finder has not been initialized properly.");
        }
    }

    /**
     * Sets bucket name for IP finder.
     *
     * @param bucketName Bucket name.
     */
    @GridSpiConfiguration(optional = false)
    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    /**
     * Sets Amazon client configuration.
     * <p>
     * For details refer to Amazon S3 API reference.
     *
     * @param cfg Amazon client configuration.
     */
    @GridSpiConfiguration(optional = true)
    public void setClientConfiguration(ClientConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * Sets AWS credentials.
     * <p>
     * For details refer to Amazon S3 API reference.
     *
     * @param cred AWS credentials.
     */
    @GridSpiConfiguration(optional = false)
    public void setAwsCredentials(AWSCredentials cred) {
        this.cred = cred;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpDiscoveryS3IpFinder.class, this, "super", super.toString());
    }
}
