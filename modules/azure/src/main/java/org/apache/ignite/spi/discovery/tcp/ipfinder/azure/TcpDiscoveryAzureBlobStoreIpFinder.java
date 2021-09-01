package org.apache.ignite.spi.discovery.tcp.ipfinder.azure;
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
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;


/**
 * Azure Blob Storage based IP Finder
 * <p>
 * For information about Blob Storage visit <a href="https://azure.microsoft.com/en-in/services/storage/blobs/">azure.microsoft.com</a>.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * <ul>
 *      <li>AccountName (see {@link #setAccountName(String)})</li>
 *      <li>AccountKey (see {@link #setAccountKey(String)})</li>
 *      <li>Account Endpoint (see {@link #setAccountEndpoint(String)})</li>
 *      <li>Container Name (see {@link #setContainerName(String)})</li>
 * </ul>
 * <h2 class="header">Optional</h2>
 * <ul>
 *      <li>Shared flag (see {@link #setShared(boolean)})</li>
 * </ul>
 * <p>
 * The finder will create a container with the provided name. The container will contain entries named
 * like the following: {@code 192.168.1.136#1001}.
 * <p>
 * Note that storing data in Azure Blob Storage service will result in charges to your Azure account.
 * Choose another implementation of {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder} for local
 * or home network tests.
 * <p>
 * Note that this finder is shared by default (see {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder#isShared()}.
 */
public class TcpDiscoveryAzureBlobStoreIpFinder extends TcpDiscoveryIpFinderAdapter {
    /** Default object's content. */
    private static final byte[] OBJECT_CONTENT = new byte[0];

    /** Grid logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Azure Blob Storage's account name*/
    private String accountName;

    /** Azure Blob Storage's account key */
    private String accountKey;

    /** End point URL */
    private String endPoint;

    /** Container name */
    private String containerName;

    /** Storage credential */
    StorageSharedKeyCredential credential;

    /** Blob service client */
    private BlobServiceClient blobServiceClient;

    /** Blob container client */
    private BlobContainerClient blobContainerClient;

    /** Init routine guard. */
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init routine latch. */
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /**
     * Default constructor
     */
    public TcpDiscoveryAzureBlobStoreIpFinder() {
        setShared(true);
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        init();

        Collection<InetSocketAddress> addrs = new ArrayList<>();
        Set<String> seenBlobNames = new HashSet<>();

        Iterator<BlobItem> blobItemIterator = blobContainerClient.listBlobs().iterator();

        while (blobItemIterator.hasNext()) {
            BlobItem blobItem = blobItemIterator.next();

            // https://github.com/Azure/azure-sdk-for-java/issues/20515
            if (seenBlobNames.contains(blobItem.getName())) {
                break;
            }

            try {
                if (!blobItem.isDeleted()) {
                    addrs.add(addrFromString(blobItem.getName()));
                    seenBlobNames.add(blobItem.getName());
                }
            }
            catch (Exception e) {
                throw new IgniteSpiException("Failed to get content from the container: " + containerName, e);
            }
        }

        return addrs;
    }

    /** {@inheritDoc} */
    @Override public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        assert !F.isEmpty(addrs);

        init();

        for (InetSocketAddress addr : addrs) {
            try {
                String key = URLEncoder.encode(keyFromAddr(addr), StandardCharsets.UTF_8.name());
                BlockBlobClient blobClient = blobContainerClient.getBlobClient(key).getBlockBlobClient();

                blobClient.upload(new ByteArrayInputStream(OBJECT_CONTENT), OBJECT_CONTENT.length);
            }
            catch (UnsupportedEncodingException e) {
                throw new IgniteSpiException("Unable to encode URL due to error " + e.getMessage(), e);
            }
            catch (BlobStorageException e) {
                // If the blob already exists, ignore
                if (e.getStatusCode() != 409)
                    throw new IgniteSpiException("Failed to upload blob with exception " + e.getMessage(), e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        assert !F.isEmpty(addrs);

        init();

        for (InetSocketAddress addr : addrs) {
            String key = keyFromAddr(addr);

            try {
                blobContainerClient.getBlobClient(key).delete();
            } catch (Exception e) {
                // https://github.com/Azure/azure-sdk-for-java/issues/20551
                if ((!(e.getMessage().contains("InterruptedException"))) || (e instanceof BlobStorageException
                    && (((BlobStorageException)e).getErrorCode() != BlobErrorCode.BLOB_NOT_FOUND))) {
                    throw new IgniteSpiException("Failed to delete entry [containerName=" + containerName +
                        ", entry=" + key + ']', e);
                }
            }
        }
    }

    /**
     * Sets Azure Blob Storage Account Name.
     * <p>
     * For details refer to Azure Blob Storage API reference.
     *
     * @param accountName Account Name
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = false)
    public TcpDiscoveryAzureBlobStoreIpFinder setAccountName(String accountName) {
        this.accountName = accountName;

        return this;
    }

    /**
     * Sets Azure Blob Storage Account Key
     * <p>
     * For details refer to Azure Blob Storage API reference.
     *
     * @param accountKey Account Key
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = false)
    public TcpDiscoveryAzureBlobStoreIpFinder setAccountKey(String accountKey) {
        this.accountKey = accountKey;

        return this;
    }

    /**
     * Sets Azure Blob Storage endpoint
     * <p>
     * For details refer to Azure Blob Storage API reference.
     *
     * @param endPoint Endpoint for storage
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = false)
    public TcpDiscoveryAzureBlobStoreIpFinder setAccountEndpoint(String endPoint) {
        this.endPoint = endPoint;

        return this;
    }

    /**
     * Sets container name for using in the context
     * If the container name doesn't exist Ignite will automatically create itß.
     *
     * @param containerName Container Name.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = false)
    public TcpDiscoveryAzureBlobStoreIpFinder setContainerName(String containerName) {
        this.containerName = containerName;

        return this;
    }

    /**
     * Initialize the IP finder
     * @throws IgniteSpiException
     */
    private void init() throws IgniteSpiException {
        if (initGuard.compareAndSet(false, true)) {
            if (accountKey == null || accountName == null || containerName == null || endPoint == null) {
                throw new IgniteSpiException(
                        "One or more of the required parameters is not set [accountName=" +
                                accountName + ", accountKey=" + accountKey + ", containerName=" +
                                containerName + ", endPoint=" + endPoint + "]");
            }

            try {
                credential = new StorageSharedKeyCredential(accountName, accountKey);
                blobServiceClient = new BlobServiceClientBuilder().endpoint(endPoint).credential(credential).buildClient();
                blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);

                if (!blobContainerClient.exists()) {
                    U.warn(log, "Container doesn't exist, will create it [containerName=" + containerName + "]");

                    blobContainerClient.create();
                }
            }
            finally {
                initLatch.countDown();
            }
        }
        else {
            try {
                U.await(initLatch);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteSpiException("Thread has been interrupted.", e);
            }

            try {
                if (!blobContainerClient.exists())
                    throw new IgniteSpiException("IpFinder has not been initialized properly");
            } catch (Exception e) {
                // Check if this is a nested exception wrapping an InterruptedException
                // https://github.com/Azure/azure-sdk-for-java/issues/20551
                if (!(e.getCause() instanceof InterruptedException)) {
                    throw e;
                }
            }
        }
    }

    /**
     * Constructs a node address from bucket's key.
     *
     * @param key Bucket key.
     * @return Node address.
     * @throws IgniteSpiException In case of error.
     */
    private InetSocketAddress addrFromString(String key) throws IgniteSpiException {
        //TODO: This needs to move out to a generic helper class
        String[] res = key.split("#");

        if (res.length != 2)
            throw new IgniteSpiException("Invalid address string: " + key);

        int port;

        try {
            port = Integer.parseInt(res[1]);
        }
        catch (NumberFormatException ignored) {
            throw new IgniteSpiException("Invalid port number: " + res[1]);
        }

        return new InetSocketAddress(res[0], port);
    }

    /**
     * Constructs bucket's key from an address.
     *
     * @param addr Node address.
     * @return Bucket key.
     */
    private String keyFromAddr(InetSocketAddress addr) {
        // TODO: This needs to move out to a generic helper class
        return addr.getAddress().getHostAddress() + "#" + addr.getPort();
    }

    /** {@inheritDoc} */
    @Override public TcpDiscoveryAzureBlobStoreIpFinder setShared(boolean shared) {
        super.setShared(shared);

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryAzureBlobStoreIpFinder.class, this);
    }
}
