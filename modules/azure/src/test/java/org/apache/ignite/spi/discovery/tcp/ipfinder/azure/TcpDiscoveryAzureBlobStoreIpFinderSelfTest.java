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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAbstractSelfTest;
import org.apache.ignite.testsuites.IgniteAzureTestSuite;

public class TcpDiscoveryAzureBlobStoreIpFinderSelfTest
        extends TcpDiscoveryIpFinderAbstractSelfTest<TcpDiscoveryAzureBlobStoreIpFinder> {
    private static String containerName;

    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public TcpDiscoveryAzureBlobStoreIpFinderSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        containerName = "ip-finder-test-container-" + InetAddress.getLocalHost().getAddress()[3];

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        try {
            BlobContainerClient container =
                new BlobServiceClientBuilder().endpoint(IgniteAzureTestSuite.getEndpoint()).credential(
                    new StorageSharedKeyCredential(IgniteAzureTestSuite.getAccountName(),
                        IgniteAzureTestSuite.getAccountKey())).buildClient().getBlobContainerClient(containerName);

            if (container.exists())
                container.delete();
        }
        catch (Exception e) {
            log.warning("Failed to remove bucket on Azure [containerName=" + containerName + ", mes=" + e.getMessage() + ']');
        }
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoveryAzureBlobStoreIpFinder ipFinder() throws Exception {
        TcpDiscoveryAzureBlobStoreIpFinder finder = new TcpDiscoveryAzureBlobStoreIpFinder();

        injectLogger(finder);

        assert finder.isShared() : "Ip finder must be shared by default.";

        finder.setAccountName(IgniteAzureTestSuite.getAccountName());
        finder.setAccountKey(IgniteAzureTestSuite.getAccountKey());
        finder.setAccountEndpoint(IgniteAzureTestSuite.getEndpoint());

        finder.setContainerName(containerName);

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
