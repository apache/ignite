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

package org.apache.ignite.spi.discovery.tcp.ipfinder.gce;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.StorageObject;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;

/**
 * Google Cloud Storage based IP finder.
 * <p>
 * For information about Cloud Storage visit <a href="https://cloud.google.com/storage/">cloud.google.com</a>.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * <ul>
 *      <li>Service Account Id (see {@link #setServiceAccountId(String)})</li>
 *      <li>Service Account P12 key file path (see {@link #setServiceAccountP12FilePath(String)})</li>
 *      <li>Google Platform project name (see {@link #setProjectName(String)})</li>
 *      <li>Google Storage bucket name (see {@link #setBucketName(String)})</li>
 * </ul>
 * <h2 class="header">Optional</h2>
 * <ul>
 *      <li>Shared flag (see {@link #setShared(boolean)})</li>
 * </ul>
 * <p>
 * The finder will create a bucket with the provided name. The bucket will contain entries named
 * like the following: {@code 192.168.1.136#1001}.
 * <p>
 * Note that storing data in Google Cloud Storage service will result in charges to your Google Cloud Platform account.
 * Choose another implementation of {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder} for local
 * or home network tests.
 * <p>
 * Note that this finder is shared by default (see {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder#isShared()}.
 */
public class TcpDiscoveryGoogleStorageIpFinder extends TcpDiscoveryIpFinderAdapter {
    /** Default object's content. */
    private final static ByteArrayInputStream OBJECT_CONTENT =  new ByteArrayInputStream(new byte[0]);

    /** Grid logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Google Cloud Platform's project name.*/
    private String projectName;

    /** Google Storage bucket name. */
    private String bucketName;

    /** Service account p12 private key file name. */
    private String srvcAccountP12FilePath;

    /** Service account id. */
    private String srvcAccountId;

    /** Google storage. */
    private Storage storage;

    /** Init routine guard. */
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init routine latch. */
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /**
     *
     */
    public TcpDiscoveryGoogleStorageIpFinder() {
        setShared(true);
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        init();

        Collection<InetSocketAddress> addrs = new ArrayList<>();

        try {
            Storage.Objects.List listObjects = storage.objects().list(bucketName);

            com.google.api.services.storage.model.Objects objects;

            do {
                objects = listObjects.execute();

                if (objects == null || objects.getItems() == null)
                    break;

                for (StorageObject object : objects.getItems())
                    addrs.add(addrFromString(object.getName()));

                listObjects.setPageToken(objects.getNextPageToken());
            }
            while (null != objects.getNextPageToken());
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to get content from the bucket: " + bucketName, e);
        }

        return addrs;
    }

    /** {@inheritDoc} */
    @Override public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        assert !F.isEmpty(addrs);

        init();

        for (InetSocketAddress addr : addrs) {
            String key = keyFromAddr(addr);

            StorageObject object = new StorageObject();

            object.setBucket(bucketName);
            object.setName(key);

            InputStreamContent content =  new InputStreamContent("application/octet-stream", OBJECT_CONTENT);

            content.setLength(OBJECT_CONTENT.available());

            try {
                Storage.Objects.Insert insertObject = storage.objects().insert(bucketName, object, content);

                insertObject.execute();
            }
            catch (Exception e) {
                throw new IgniteSpiException("Failed to put entry [bucketName=" + bucketName +
                    ", entry=" + key + ']', e);
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
                Storage.Objects.Delete deleteObject = storage.objects().delete(bucketName, key);

                deleteObject.execute();
            }
            catch (Exception e) {
                throw new IgniteSpiException("Failed to delete entry [bucketName=" + bucketName +
                    ", entry=" + key + ']', e);
            }
        }
    }

    /**
     * Sets Google Cloud Platforms project name.
     * Usually this is an auto generated project number (ex. 208709979073) that can be found in "Overview" section
     * of Google Developer Console.
     * <p>
     * For details refer to Google Cloud Platform API reference.
     *
     * @param projectName Project name.
     */
    @IgniteSpiConfiguration(optional = false)
    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    /**
     * Sets Google Cloud Storage bucket name.
     * If the bucket doesn't exist Ignite will automatically create it. However the name must be unique across whole
     * Google Cloud Storage and Service Account Id (see {@link #setServiceAccountId(String)}) must be authorized to
     * perform this operation.
     *
     * @param bucketName Bucket name.
     */
    @IgniteSpiConfiguration(optional = false)
    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }


    /**
     * Sets a full path to the private key in PKCS12 format of the Service Account.
     * <p>
     * For more information please refer to
     * <a href="https://cloud.google.com/storage/docs/authentication#service_accounts">
     *     Service Account Authentication</a>.
     *
     * @param p12FileName Private key file full path.
     */
    @IgniteSpiConfiguration(optional = false)
    public void setServiceAccountP12FilePath(String p12FileName) {
        this.srvcAccountP12FilePath = p12FileName;
    }

    /**
     * Sets the service account ID (typically an e-mail address).
     * <p>
     * For more information please refer to
     * <a href="https://cloud.google.com/storage/docs/authentication#service_accounts">
     *     Service Account Authentication</a>.
     *
     * @param id Service account ID.
     */
    @IgniteSpiConfiguration(optional = false)
    public void setServiceAccountId(String id) {
        this.srvcAccountId = id;
    }

    /**
     * Google Cloud Storage initialization.
     *
     * @throws IgniteSpiException In case of error.
     */
    private void init() throws IgniteSpiException {
        if (initGuard.compareAndSet(false, true)) {
            if (srvcAccountId == null ||
                srvcAccountP12FilePath == null ||
                projectName == null ||
                bucketName == null) {
                throw new IgniteSpiException(
                    "One or more of the required parameters is not set [serviceAccountId=" +
                        srvcAccountId + ", serviceAccountP12FilePath=" + srvcAccountP12FilePath + ", projectName=" +
                        projectName + ", bucketName=" + bucketName + "]");
            }

            try {
                NetHttpTransport httpTransport;

                try {
                    httpTransport = GoogleNetHttpTransport.newTrustedTransport();
                }
                catch (GeneralSecurityException | IOException e) {
                    throw new IgniteSpiException(e);
                }

                GoogleCredential cred;

                try {
                    cred = new GoogleCredential.Builder().setTransport(httpTransport)
                        .setJsonFactory(JacksonFactory.getDefaultInstance()).setServiceAccountId(srvcAccountId)
                        .setServiceAccountPrivateKeyFromP12File(new File(srvcAccountP12FilePath))
                        .setServiceAccountScopes(Collections.singleton(StorageScopes.DEVSTORAGE_FULL_CONTROL)).build();

                }
                catch (Exception e) {
                    throw new IgniteSpiException("Failed to authenticate on Google Cloud Platform", e);
                }

                try {
                    storage = new Storage.Builder(httpTransport, JacksonFactory.getDefaultInstance(), cred)
                        .setApplicationName(projectName).build();
                }
                catch (Exception e) {
                    throw new IgniteSpiException("Failed to open a storage for given project name: " + projectName, e);
                }

                boolean createBucket = false;

                try {
                    Storage.Buckets.Get getBucket = storage.buckets().get(bucketName);

                    getBucket.setProjection("full");

                    getBucket.execute();
                }
                catch (GoogleJsonResponseException e) {
                    if (e.getStatusCode() == 404) {
                        U.warn(log, "Bucket doesn't exist, will create it [bucketName=" + bucketName + "]");

                        createBucket = true;
                    }
                    else
                        throw new IgniteSpiException("Failed to open the bucket: " + bucketName, e);
                }
                catch (Exception e) {
                    throw new IgniteSpiException("Failed to open the bucket: " + bucketName, e);
                }

                if (createBucket) {
                    Bucket newBucket = new Bucket();

                    newBucket.setName(bucketName);

                    try {
                        Storage.Buckets.Insert insertBucket = storage.buckets().insert(projectName, newBucket);

                        insertBucket.setProjection("full");
                        insertBucket.setPredefinedDefaultObjectAcl("projectPrivate");

                        insertBucket.execute();
                    }
                    catch (Exception e) {
                        throw new IgniteSpiException("Failed to create the bucket: " + bucketName, e);
                    }
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

            if (storage == null)
                throw new IgniteSpiException("IpFinder has not been initialized properly");
        }
    }

    /**
     * Constructs bucket's key from an address.
     *
     * @param addr Node address.
     * @return Bucket key.
     */
    private String keyFromAddr(InetSocketAddress addr) {
        return addr.getAddress().getHostAddress() + "#" +  addr.getPort();
    }

    /**
     * Constructs a node address from bucket's key.
     *
     * @param key Bucket key.
     * @return Node address.
     * @throws IgniteSpiException In case of error.
     */
    private InetSocketAddress addrFromString(String key) throws IgniteSpiException {
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
     * Used by TEST SUITES only. Called through reflection.
     *
     * @param bucketName Bucket to delete.
     */
    private void removeBucket(String bucketName) {
        init();

        try {
            Storage.Buckets.Delete deleteBucket = storage.buckets().delete(bucketName);

            deleteBucket.execute();
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to remove the bucket: " + bucketName, e);
        }
    }
}
