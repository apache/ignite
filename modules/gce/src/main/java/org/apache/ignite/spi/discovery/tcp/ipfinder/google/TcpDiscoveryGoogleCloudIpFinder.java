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

package org.apache.ignite.spi.discovery.tcp.ipfinder.google;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.*;
import com.google.common.collect.ImmutableMap;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;

import com.google.api.client.googleapis.auth.oauth2.*;
import com.google.api.client.json.jackson2.*;

import java.io.*;
import java.net.*;
import java.security.GeneralSecurityException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Google Cloud Storage based IP finder.
 *
 * TODO: complete
 */
public class TcpDiscoveryGoogleCloudIpFinder extends TcpDiscoveryIpFinderAdapter {
    /* Default object's content. */
    private final static ByteArrayInputStream OBJECT_CONTENT =  new ByteArrayInputStream(new byte[1]);

    /* Google Cloud Platform's project name.*/
    private String projectName;

    /* Google Cloud Platform's bucket name. */
    private String bucketName;

    /* Service account p12 private key file name. */
    private String serviceAccountP12FilePath;

    /* Service account id. */
    private String serviceAccountId;

    /* Google storage. */
    private Storage storage;

    /* Init routine guard. */
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /* Init routine latch. */
    private final CountDownLatch initLatch = new CountDownLatch(1);

    public TcpDiscoveryGoogleCloudIpFinder() {
        setShared(true);
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        init();

        Collection<InetSocketAddress> addrs = new LinkedList<>();

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
            } while (null != objects.getNextPageToken());
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
            } catch (Exception e) {
                throw new IgniteSpiException("Failed to delete entry [bucketName=" + bucketName +
                                                     ", entry=" + key + ']', e);
            }
        }
    }

    /**
     * Sets Google Cloud Platforms project name.
     * The project name is the one which your Google VM instances, Cloud Storage, etc. belong to.
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
     * Sets Google Cloud Platforms bucket name.
     *
     * @param bucketName Bucket name.
     */
    @IgniteSpiConfiguration(optional = false)
    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    @IgniteSpiConfiguration(optional = false)
    public void setServiceAccountP12FilePath(String p12FileName) {
        this.serviceAccountP12FilePath = p12FileName;
    }

    @IgniteSpiConfiguration(optional = false)
    public void setServiceAccountId(String id) {
        this.serviceAccountId = id;
    }

    /**
     *
     */
    private void init() throws IgniteSpiException {
        if (initGuard.compareAndSet(false, true)) {
            try {
                NetHttpTransport httpTransport;

                try {
                    httpTransport = GoogleNetHttpTransport.newTrustedTransport();
                } catch (GeneralSecurityException | IOException e) {
                    throw new IgniteSpiException(e);
                }

                GoogleCredential credential;

                try {
                    credential = new GoogleCredential.Builder().setTransport(httpTransport)
                        .setJsonFactory(JacksonFactory.getDefaultInstance()).setServiceAccountId(serviceAccountId)
                        .setServiceAccountPrivateKeyFromP12File(new File(serviceAccountP12FilePath))
                        .setServiceAccountScopes(Collections.singleton(StorageScopes.DEVSTORAGE_READ_WRITE)).build();
                } catch (Exception e) {
                    throw new IgniteSpiException("Failed to authenticate on Google Cloud Platform", e);
                }

                try {
                    storage = new Storage.Builder(httpTransport, JacksonFactory.getDefaultInstance(), credential)
                                      .setApplicationName(projectName).build();
                } catch (Exception e) {
                    throw new IgniteSpiException("Failed to open a storage for given project name: " + projectName);
                }
            }
            finally {
                initLatch.countDown();
            }
        }
        else {
            try {
                U.await(initLatch);
            } catch (IgniteInterruptedCheckedException e) {
                throw new IgniteSpiException("Thread has been interrupted.", e);
            }

            if (storage == null)
                throw new IgniteSpiException("IpFinder has not been initialized properly");
        }
    }

    private String keyFromAddr(InetSocketAddress addr) {
        return addr.getAddress().getHostAddress() + ":" +  addr.getPort();
    }

    private InetSocketAddress addrFromString(String address) throws IgniteSpiException {
        String[] res = address.split(":");

        if (res.length != 2)
            throw new IgniteSpiException("Invalid address string: " + address);


        int port;
        try {
            port = Integer.parseInt(res[1]);
        }
        catch (NumberFormatException e) {
            throw new IgniteSpiException("Invalid port number: " + res[1]);
        }

        return new InetSocketAddress(res[0], port);
    }



    public static void main(String args[]) {
        TcpDiscoveryGoogleCloudIpFinder ipFinder = new TcpDiscoveryGoogleCloudIpFinder();

        String bucketName = "grid-gain-test-bucket1";

        ipFinder.setBucketName(bucketName);
        ipFinder.setProjectName("gridgain");
        ipFinder.setServiceAccountId("208709979073-v0mn6ttpd3mqu2b5lbhh1mvdet7os3n6@developer.gserviceaccount.com");
        ipFinder.setServiceAccountP12FilePath("C:\\ignite\\GCE\\gridgain-0889e44b58b7.p12");

        List<InetSocketAddress> addresses = new LinkedList<>();
        addresses.add(new InetSocketAddress("192.168.0.1", 23));
        addresses.add(new InetSocketAddress("192.168.0.1", 89));
        addresses.add(new InetSocketAddress("92.68.0.1", 1223));

        System.out.println("PUT ADDR");
        ipFinder.registerAddresses(addresses);

        Collection<InetSocketAddress> result = ipFinder.getRegisteredAddresses();
        System.out.println("GET ADDR");

        for (InetSocketAddress add: result) {
            System.out.println(add.getAddress().getHostAddress() + ":" + add.getPort());
        }

        System.out.println("REMOVE");
        ipFinder.unregisterAddresses(addresses);


        result = ipFinder.getRegisteredAddresses();
        System.out.println("GET ADDR 2");

        for (InetSocketAddress add: result) {
            System.out.println(add.getAddress().getHostAddress() + ":" + add.getPort());
        }

        List<InetSocketAddress> addresses2 = new LinkedList<>();
        addresses.add(new InetSocketAddress("192.1638.02.1", 23));
        ipFinder.unregisterAddresses(addresses2);
    }


}
