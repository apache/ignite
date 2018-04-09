package org.apache.ignite.configuration;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

public class S3AddressResolver implements AddressResolver {
    public static final String DELIM = "#";
    private Map<InetSocketAddress, InetSocketAddress> inetSockAddrMap = new HashMap<>();
    private AWSCredentials cred;
    private AWSCredentialsProvider credProvider;
    private AmazonS3 s3;
    private String bucketName;
    private boolean doCache = true;
    @Nullable
    private String bucketEndpoint;
    @Nullable
//    private String sseAlg;
//    @Nullable
    private String region;

    @LoggerResource
    private IgniteLogger log;

    public S3AddressResolver(AWSCredentials cred, String bucketName, @Nullable String bucketEndpoint) {
        this(cred, bucketName, bucketEndpoint, true);
    }

    public S3AddressResolver(AWSCredentialsProvider credProvider, String bucketName, @Nullable String bucketEndpoint) {
        this(credProvider, bucketName, bucketEndpoint, true);
    }

    public S3AddressResolver(AWSCredentials cred, String bucketName, @Nullable String bucketEndpoint, boolean doCache) {
        this.cred = cred;
        if (this.credProvider == null) {
            this.credProvider = new AWSStaticCredentialsProvider(this.cred);
        }
        this.bucketName = bucketName;
        this.bucketEndpoint = bucketEndpoint;
        this.doCache = doCache;
        //this.sseAlg = sseAlg;
        init();
    }

    public S3AddressResolver(AWSCredentialsProvider credProvider, String bucketName, @Nullable String bucketEndpoint, boolean doCache) {
        this.credProvider = credProvider;
        this.bucketName = bucketName;
        this.bucketEndpoint = bucketEndpoint;
        this.doCache = doCache;
        //this.sseAlg = sseAlg;
        init();
    }

    private void init() {
        if (this.cred == null && this.credProvider == null) {
            throw new IgniteSpiException("AWS credentials are not set.");
        }
        if (F.isEmpty(this.bucketName)) {
            throw new IgniteSpiException("Bucket name is null or empty (provide bucket name and restart).");
        }

        if (F.isEmpty(this.region)) {
            this.region = Regions.US_WEST_1.getName();
        }

        this.s3 = this.createAmazonS3Client();
        if (!this.s3.doesBucketExist(this.bucketName))
            throw new IgniteSpiException("Bucket not exists [Bucket =" + bucketName);

    }


    private String key(InetSocketAddress addr) {
        assert addr != null;
        SB sb = new SB();
        sb.a(addr.getAddress().getHostAddress()).a(DELIM).a(addr.getPort());
        return sb.toString();
    }

    private AmazonS3 createAmazonS3Client() {

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(credProvider)
                .build();
        if (!F.isEmpty(this.bucketEndpoint)) {
            s3Client.setEndpoint(this.bucketEndpoint);
        }
        return s3Client;
    }

    private InetSocketAddress getResolvedAddress(InetSocketAddress inetSocketAddress) throws IgniteCheckedException {
        InetSocketAddress mapAddress = null;
        if (doCache) {
            mapAddress = inetSockAddrMap.get(inetSocketAddress);
        }

        if (!Optional.of(mapAddress).isPresent()) {
            S3ObjectInputStream s3ObjectInputStream = null;
            try {
                s3ObjectInputStream = s3.getObject(this.bucketName, key(inetSocketAddress)).getObjectContent();
                mapAddress = InetSocketAddress.createUnresolved(new String(IOUtils.toByteArray(s3ObjectInputStream)), inetSocketAddress.getPort());
                if (doCache)
                    inetSockAddrMap.put(inetSocketAddress, mapAddress);
            } catch (IOException e) {
                throw new IgniteCheckedException(e);
            } finally {
                if (s3ObjectInputStream != null) {
                    try {
                        s3ObjectInputStream.close();
                    } catch (IOException e) {
                        log.warning("S3 getObject stream closing failed.");
                    }
                }
            }
        }
        return mapAddress;
    }

    @Override
    public Collection<InetSocketAddress> getExternalAddresses(InetSocketAddress inetSocketAddress) throws IgniteCheckedException {
        return Collections.singleton(getResolvedAddress(inetSocketAddress));
    }
}
