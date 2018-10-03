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

package org.apache.ignite.spi.discovery.tcp.ipfinder.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
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
import org.jetbrains.annotations.Nullable;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import java.io.IOException;
import java.util.*;

/**
 * AWS S3-based IP finder.
 * <p>
 * For information about Amazon S3 visit <a href="http://aws.amazon.com">aws.amazon.com</a>.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * <ul>
 * <li>AWS credentials (see {@link #setAwsCredentials(AWSCredentials)} and
 * {@link #setAwsCredentialsProvider(AWSCredentialsProvider)}</li>
 * <li>Bucket name (see {@link #setBucketName(String)})</li>
 * </ul>
 * <h2 class="header">Optional</h2>
 * <ul>
 * <li>Client configuration (see {@link #setClientConfiguration(ClientConfiguration)})</li>
 * <li>Shared flag (see {@link #setShared(boolean)})</li>
 * <li>Bucket endpoint (see {@link #setBucketEndpoint(String)})</li>
 * <li>Region (see {@link #setRegion(String)})</li>
 * <li>Server side encryption algorithm (see {@link #setSSEAlgorithm(String)})</li>
 *      <li>Server side encryption algorithm (see {@link #setKeyPrefix(String)})</li>
 * </ul>
 * <p>
 * The finder will create S3 bucket with configured name. The bucket will contain entries named
 * like the following: {@code 192.168.1.136#1001}.
 * <p>
 * Note that storing data in AWS S3 service will result in charges to your AWS account.
 * Choose another implementation of {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder} for local
 * or home network tests.
 * <p>
 * Note that this finder is shared by default (see {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder#isShared()}.
 */

public class TcpDiscoveryS3IpFinder extends TcpDiscoveryIpFinderAdapter {
    private static final String DELIM = "#";
    @GridToStringExclude
    private static HttpClient httpClient = HttpClientBuilder.create().build();
    @GridToStringExclude
    private final ObjectMetadata objMetadata = new ObjectMetadata();
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();
    @GridToStringExclude
    private final CountDownLatch initLatch = new CountDownLatch(1);
    private byte[] entryContent = new byte[]{1};
    @LoggerResource
    private IgniteLogger log;
    @GridToStringExclude
    private AmazonS3 s3;
    private String bucketName;

    /** Bucket endpoint. */
    @Nullable private String bucketEndpoint;

    /** Server side encryption algorithm. */
    @Nullable private String sseAlg;

    /** Sub-folder name to write node addresses. */
    @Nullable private String keyPrefix;

    /** Init guard. */
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init latch. */
    @GridToStringExclude
    private boolean isPublicIpMapRequired = false;
    @Nullable
    private String bucketEndpoint;
    @Nullable
    private String region;
    @Nullable
    private String sseAlg;
    private ClientConfiguration cfg;
    @GridToStringExclude
    private AWSCredentials cred;
    @GridToStringExclude
    private AWSCredentialsProvider credProvider;

    public TcpDiscoveryS3IpFinder() {
        this.setShared(true);
    }

    public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        this.initClient();
        LinkedList addrs = new LinkedList();

        try {
            ObjectListing list;

            if (keyPrefix == null)
                list = s3.listObjects(bucketName);
            else
                list = s3.listObjects(bucketName, keyPrefix);

            while (true) {
                Iterator var3 = list.getObjectSummaries().iterator();

                while (var3.hasNext()) {
                    S3ObjectSummary sum = (S3ObjectSummary) var3.next();
                    String key = sum.getKey();
                    String addr = key;

                    if (keyPrefix != null)
                        addr = key.replaceFirst(Pattern.quote(keyPrefix), "");

                    StringTokenizer st = new StringTokenizer(addr, DELIM);

                    if (st.countTokens() != 2)
                        U.error(log, "Failed to parse S3 entry due to invalid format: " + addr);
                    else {
                        String addrStr = st.nextToken();
                        String portStr = st.nextToken();
                        int port = -1;

                        try {
                            port = Integer.parseInt(portStr);
                        }
                        catch (NumberFormatException e) {
                            U.error(log, "Failed to parse port for S3 entry: " + addr, e);
                        }

                        if (port != -1) {
                            try {
                                addrs.add(new InetSocketAddress(addrStr, port));
                            } catch (IllegalArgumentException var11) {
                                U.error(this.log, "Failed to parse port for S3 entry: " + key, var11);
                            }
                            catch (IllegalArgumentException e) {
                                U.error(log, "Failed to parse port for S3 entry: " + addr, e);
                            }
                    }
                }

                if (!list.isTruncated()) {
                    return addrs;
                }

                list = this.s3.listNextBatchOfObjects(list);
            }
        } catch (AmazonClientException var13) {
            throw new IgniteSpiException("Failed to list objects in the bucket: " + this.bucketName, var13);
        }
    }

    public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        assert !F.isEmpty(addrs);

        this.initClient();
        Iterator var2 = addrs.iterator();

        while (var2.hasNext()) {
            InetSocketAddress addr = (InetSocketAddress) var2.next();
            String key = this.key(addr);
            try {
                this.s3.putObject(this.bucketName, key, new ByteArrayInputStream(getEntryContents()), this.objMetadata);
            } catch (AmazonClientException var6) {
                throw new IgniteSpiException("Failed to put entry [bucketName=" + this.bucketName + ", entry=" + key + ']', var6);
            }
        }

    }

    public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        assert !F.isEmpty(addrs);

        this.initClient();
        Iterator var2 = addrs.iterator();

        while (var2.hasNext()) {
            InetSocketAddress addr = (InetSocketAddress) var2.next();
            String key = this.key(addr);

            try {
                this.s3.deleteObject(this.bucketName, key);
            } catch (AmazonClientException var6) {
                throw new IgniteSpiException("Failed to delete entry [bucketName=" + this.bucketName + ", entry=" + key + ']', var6);
            }
        }

    }

    private byte[] getEntryContents() {
        try {
            if (isPublicIpMapRequired) {
                entryContent = httpClient.execute(new HttpGet("http://169.254.169.254/latest/meta-data/public-ipv4")).getEntity().getContent().toString().getBytes();
            }
            return entryContent;
        } catch (ClientProtocolException e) {
            throw new IgniteSpiException("Failed to get public IP", e);
        } catch (IOException e) {
            throw new IgniteSpiException("Failed to get public IP", e);
        }
    }

    private String key(InetSocketAddress addr) {
        assert addr != null;

        SB sb = new SB();

        String addrStr = addr.getAddress().getHostAddress();

        if (keyPrefix != null)
            sb.a(keyPrefix);

        sb.a(addrStr)
            .a(DELIM)
            .a(addr.getPort());

        return sb.toString();
    }

    private void initClient() throws IgniteSpiException {
        if (this.initGuard.compareAndSet(false, true)) {
            try {
                if (this.cred == null && this.credProvider == null) {
                    throw new IgniteSpiException("AWS credentials are not set.");
                }

                if (this.cfg == null) {
                    U.warn(this.log, "Amazon client configuration is not set (will use default).");
                }

                if (F.isEmpty(this.bucketName)) {
                    throw new IgniteSpiException("Bucket name is null or empty (provide bucket name and restart).");
                }

                this.isPublicIpMapRequired = Optional.of(this.isPublicIpMapRequired).orElse(false);

                this.objMetadata.setContentLength((long) getEntryContents().length);
                if (!F.isEmpty(this.sseAlg)) {
                    this.objMetadata.setSSEAlgorithm(this.sseAlg);
                }

                this.s3 = this.createAmazonS3Client();
                if (!this.s3.doesBucketExist(this.bucketName)) {
                    try {
                        this.s3.createBucket(this.bucketName);
                        if (this.log.isDebugEnabled()) {
                            this.log.debug("Created S3 bucket: " + this.bucketName);
                        }

                        while (!this.s3.doesBucketExist(this.bucketName)) {
                            try {
                                U.sleep(200L);
                            } catch (IgniteInterruptedCheckedException var8) {
                                throw new IgniteSpiException("Thread has been interrupted.", var8);
                            }
                        }
                    } catch (AmazonClientException var9) {
                        if (!this.s3.doesBucketExist(this.bucketName)) {
                            this.s3 = null;
                            throw new IgniteSpiException("Failed to create bucket: " + this.bucketName, var9);
                        }
                    }
                }
            } finally {
                this.initLatch.countDown();
            }
        } else {
            try {
                U.await(this.initLatch);
            } catch (IgniteInterruptedCheckedException var7) {
                throw new IgniteSpiException("Thread has been interrupted.", var7);
            }

            if (this.s3 == null) {
                throw new IgniteSpiException("Ip finder has not been initialized properly.");
            }
        }

//    /**
//     * Instantiates {@code AmazonS3Client} instance.
//     *
//     * @return Client instance to use to connect to AWS.
//     */
//    AmazonS3Client createAmazonS3Client() {
//        AmazonS3Client cln = cfg != null
//            ? (cred != null ? new AmazonS3Client(cred, cfg) : new AmazonS3Client(credProvider, cfg))
//            : (cred != null ? new AmazonS3Client(cred) : new AmazonS3Client(credProvider));
//    }

    private AmazonS3 createAmazonS3Client() {
        if (this.credProvider == null) {
            this.credProvider = new AWSStaticCredentialsProvider(this.cred);
        }
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(credProvider)
                .build();
        if (!F.isEmpty(this.bucketEndpoint)) {
            s3Client.setEndpoint(this.bucketEndpoint);
        }

        if (!F.isEmpty(this.region)) {
            s3Client.setRegion(Region.getRegion(Regions.fromName(this.region)));













































        }


        return s3Client;
    }


    @IgniteSpiConfiguration(
            optional = true
    )
    public TcpDiscoveryS3IpFinder setRegion(String region) {
        this.region = region;
        return this;
    }


    @IgniteSpiConfiguration(
            optional = true
    )
    public TcpDiscoveryS3IpFinder setPublicIpMapRequired(boolean publicIpMapRequired) {
        isPublicIpMapRequired = publicIpMapRequired;
        return this;
    }

    @IgniteSpiConfiguration(
            optional = false
    )
    public TcpDiscoveryS3IpFinder setBucketName(String bucketName) {
        this.bucketName = bucketName;
        return this;
    }

    /**
     * Sets bucket endpoint for IP finder. If the endpoint is not set then IP finder will go to each region to find a
     * corresponding bucket. For information about possible endpoint names visit
     * <a href="http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region">docs.aws.amazon.com</a>.
     *
     * @param bucketEndpoint Bucket endpoint, for example, s3.us-east-2.amazonaws.com.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoveryS3IpFinder setBucketEndpoint(String bucketEndpoint) {
        this.bucketEndpoint = bucketEndpoint;
        return this;
    }

    /**
     * Sets server-side encryption algorithm for Amazon S3-managed encryption keys. For information about possible
     * S3-managed encryption keys visit
     * <a href="http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html">docs.aws.amazon.com</a>.
     *
     * @param sseAlg Server-side encryption algorithm, for example, AES256 or SSES3.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoveryS3IpFinder setSSEAlgorithm(String sseAlg) {
        this.sseAlg = sseAlg;
        return this;
    }

    @IgniteSpiConfiguration(
            optional = true
    )
    public TcpDiscoveryS3IpFinder setClientConfiguration(ClientConfiguration cfg) {
        this.cfg = cfg;
        return this;
    }

    @IgniteSpiConfiguration(
            optional = false
    )
    public TcpDiscoveryS3IpFinder setAwsCredentials(AWSCredentials cred) {
        this.cred = cred;
        return this;
    }

    @IgniteSpiConfiguration(
            optional = false
    )
    public TcpDiscoveryS3IpFinder setAwsCredentialsProvider(AWSCredentialsProvider credProvider) {
        this.credProvider = credProvider;

        return this;
    }

    public TcpDiscoveryS3IpFinder setShared(boolean shared) {
        super.setShared(shared);
        return this;
    }

    public String toString() {
        return S.toString(TcpDiscoveryS3IpFinder.class, this, "super", super.toString());
    }
}