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
import com.google.api.client.http.javanet.NetHttpTransport;
import org.apache.ignite.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;

import com.google.api.client.auth.oauth2.*;
import com.google.api.client.googleapis.auth.oauth2.*;
import com.google.api.client.json.jackson2.*;

import java.io.*;
import java.net.*;
import java.security.GeneralSecurityException;
import java.util.*;

/**
 * Google Cloud Storage based IP finder.
 *
 * TODO: complete
 */
public class TcpDiscoveryGoogleCloudIpFinder extends TcpDiscoveryIpFinderAdapter {
    /** Grid logger. */
    @LoggerResource
    private IgniteLogger log;

    /* Google Cloud Platform's project name.*/
    private String projectName;

    /* Google Cloud Platform's bucket name. */
    private String bucketName;

    /* Google Cloud Platform's secret file name. */
    private String secretsFileName;

    /* Google HTTP transport. */
    private NetHttpTransport httpTransport;

    public TcpDiscoveryGoogleCloudIpFinder() {
        setShared(true);
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {

    }

    /** {@inheritDoc} */
    @Override public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {

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

    /**
     * File name of a client secrets file that is used to authenticate with Google Cloud Platform.
     * Client secrets (JSON) file can be created and downloaded from the Google Developers Console.
     * Note that the file must correspond to a Service Account.
     * <p>
     * For details refer to Google Cloud Platform Documentation.
     *
     * @param secretsFileName Client secrets file name.
     */
    @IgniteSpiConfiguration(optional = false)
    public void setSecretsFileName(String secretsFileName) {
        this.secretsFileName = secretsFileName;
    }

    /**
     *
     */
    private AuthorizationCodeInstalledApp Credential authorize() throws IgniteSpiException {
        GoogleClientSecrets clientSecrets = null;

        try {
            clientSecrets = GoogleClientSecrets.load(JacksonFactory.getDefaultInstance(),
                new InputStreamReader(new FileInputStream(new File(secretsFileName))));

            if (clientSecrets.getDetails().getClientId() == null ||
                    clientSecrets.getDetails().getClientSecret() == null)
                throw new IgniteSpiException("Client secrets file is not well formed.");

        } catch (Exception e) {
            throw new IgniteSpiException("Failed to load client secrets JSON file.", e);
        }

        try {
            httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        } catch (GeneralSecurityException | IOException e ) {
            throw new IgniteSpiException(e);
        }

        // Set up authorization code flow.
        // Ask for only the permissions you need. Asking for more permissions will
        // reduce the number of users who finish the process for giving you access
        // to their accounts. It will also increase the amount of effort you will
        // have to spend explaining to users what you are doing with their data.
        // Here we are listing all of the available scopes. You should remove scopes
        // that you are not actually using.
        Set<String> scopes = new HashSet<String>();
        scopes.add(StorageScopes.DEVSTORAGE_FULL_CONTROL);
        scopes.add(StorageScopes.DEVSTORAGE_READ_ONLY);
        scopes.add(StorageScopes.DEVSTORAGE_READ_WRITE);

        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
            httpTransport, JSON_FACTORY, clientSecrets, scopes)
                                                   .setDataStoreFactory(dataStoreFactory)
                                                   .build();

        GooglePromtReceiver
        // Authorize.
        VerificationCodeReceiver receiver =
                AUTH_LOCAL_WEBSERVER ? new LocalServerReceiver() : new GooglePromptReceiver();
        return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
    }
}
