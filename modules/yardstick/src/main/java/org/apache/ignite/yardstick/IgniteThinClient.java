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

package org.apache.ignite.yardstick;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.client.SslProtocol;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.yardstick.io.FileUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.UrlResource;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 * Thin client.
 */
public class IgniteThinClient {
    /** Property to overwrite default 10800 port to connect */
    private static final String THIN_CLIENT_SERVER_PORT = "THIN_CLIENT_SERVER_PORT";

    /** Bind specific server host to connect (otherwise wiil be selected from SERVER_HOSTS) */
    private static final String THIN_CLIENT_SERVER_HOST = "THIN_CLIENT_SERVER_HOST";

    /** User */
    public static final String USER = "USER";

    /** Password */
    public static final String PASSWORD = "PASSWORD";

    /** SSL key store path */
    private static final String SSL_KEYSTORE_PATH = "SSL_KEYSTORE_PATH";

    /** SSL trust store path */
    private static final String SSL_TRUSTSTORE_PATH = "SSL_TRUSTSTORE_PATH";

    /** SSL key store password */
    private static final String SSL_KEYSTORE_PASSWORD = "SSL_KEYSTORE_PASSWORD";

    /** SSL trust store password */
    private static final String SSL_TRUSTSTORE_PASSWORD = "SSL_TRUSTSTORE_PASSWORD";

    /** (Optional) Key store type (default JKS) */
    private static final String SSL_KEY_STORE_TYPE = "SSL_KEY_STORE_TYPE";

    /** (Optional) Trust store type (default JKS) */
    private static final String SSL_TRUST_STORE_TYPE = "SSL_TRUST_STORE_TYPE";

    /** (Optional) Key algorithm (default SunX509) */
    private static final String SSL_KEY_ALGORITHM = "SSL_KEY_ALGORITHM";

    /** (Optional) Trust all (default false) */
    private static final String SSL_TRUST_ALL = "SSL_TRUST_ALL";

    /** (Optional) SSL protocol (default TLS) */
    private static final String SSL_PROTOCOL = "SSL_PROTOCOL";

    /** Default trustAll */
    private static final String DEFAULT_TRUST_ALL_STRING = "false";

    /** Thin client. */
    private IgniteClient client;

    /** */
    public IgniteThinClient() {
        // No-op.
    }

    /**
     * @param client Use exist ignite client.
     */
    public IgniteThinClient(IgniteClient client) {
        this.client = client;
    }

    /** {@inheritDoc} */
    public IgniteClient start(BenchmarkConfiguration cfg, String host) throws Exception {
        IgniteBenchmarkArguments args = new IgniteBenchmarkArguments();

        BenchmarkUtils.jcommander(cfg.commandLineArguments(), args, "<ignite-node>");

        IgniteBiTuple<IgniteConfiguration, ? extends ApplicationContext> tup = loadConfiguration(args.configuration());

        IgniteConfiguration c = tup.get1();

        assert c != null;

        if (args.cleanWorkDirectory())
            FileUtils.cleanDirectory(U.workDirectory(c.getWorkDirectory(), c.getIgniteHome()));

        ClientConfiguration clCfg = new ClientConfiguration();

        // get predefined client hosts
        String port = cfg.customProperties().getOrDefault(THIN_CLIENT_SERVER_PORT,
            String.valueOf(ClientConnectorConfiguration.DFLT_PORT));

        // overwrite server host to connect thin client
        if (cfg.customProperties().containsKey(THIN_CLIENT_SERVER_HOST) &&
            cfg.customProperties().get(THIN_CLIENT_SERVER_HOST) != null &&
            !cfg.customProperties().get(THIN_CLIENT_SERVER_HOST).isEmpty())
            host = cfg.customProperties().get(THIN_CLIENT_SERVER_HOST);

        String hostPort = String.format("%s:%s", host, port);

        BenchmarkUtils.println(String.format("Using for connection address: %s", hostPort));

        clCfg.setAddresses(hostPort);

        // set authentication if needed
        boolean authenticationEnabled = cfg.customProperties().containsKey(USER) &&
            cfg.customProperties().containsKey(PASSWORD);

        if (authenticationEnabled) {
            String username = cfg.customProperties().get(USER);

            String pwd = cfg.customProperties().get(PASSWORD);

            clCfg.setUserName(username)
                .setUserPassword(pwd);
        }

        // set SSL if needed
        boolean trustAll = Boolean.valueOf(cfg.customProperties().getOrDefault(SSL_TRUST_ALL, DEFAULT_TRUST_ALL_STRING));

        boolean trustStoreDefined = cfg.customProperties().containsKey(SSL_TRUSTSTORE_PASSWORD) &&
            cfg.customProperties().containsKey(SSL_TRUSTSTORE_PATH);

        boolean isSecuredConnection = cfg.customProperties().containsKey(SSL_KEYSTORE_PASSWORD) &&
            cfg.customProperties().containsKey(SSL_KEYSTORE_PATH) &&
            // if trustAll == true or trustStore params defined
            (trustStoreDefined || trustAll);

        if (isSecuredConnection) {
            String keyStore = cfg.customProperties().get(SSL_KEYSTORE_PATH);

            String keyStorePwd = cfg.customProperties().get(SSL_KEYSTORE_PASSWORD);

            String keyStoreType = cfg.customProperties().getOrDefault(SSL_KEY_STORE_TYPE, SslContextFactory.DFLT_STORE_TYPE);

            String keyAlg = cfg.customProperties().getOrDefault(SSL_KEY_ALGORITHM, SslContextFactory.DFLT_KEY_ALGORITHM);

            SslProtocol protocol = SslProtocol.valueOf(cfg.customProperties().getOrDefault(SSL_PROTOCOL, SslContextFactory.DFLT_SSL_PROTOCOL));

            if (trustStoreDefined) {
                String trustStore = cfg.customProperties().get(SSL_TRUSTSTORE_PATH);

                String trustStorePwd = cfg.customProperties().get(SSL_TRUSTSTORE_PASSWORD);

                String trustStoreType = cfg.customProperties().getOrDefault(SSL_TRUST_STORE_TYPE, SslContextFactory.DFLT_STORE_TYPE);

                clCfg.setSslTrustCertificateKeyStorePath(trustStore)
                    .setSslTrustCertificateKeyStoreType(trustStoreType)
                    .setSslTrustCertificateKeyStorePassword(trustStorePwd);
            }

            clCfg.setSslMode(SslMode.REQUIRED)
                .setSslClientCertificateKeyStorePath(keyStore)
                .setSslClientCertificateKeyStoreType(keyStoreType)
                .setSslClientCertificateKeyStorePassword(keyStorePwd)
                .setSslKeyAlgorithm(keyAlg)
                .setSslTrustAll(trustAll)
                .setSslProtocol(protocol);
        }

        client = Ignition.startClient(clCfg);

        return client;
    }

    /**
     * @param springCfgPath Spring configuration file path.
     * @return Tuple with grid configuration and Spring application context.
     * @throws Exception If failed.
     */
    private static IgniteBiTuple<IgniteConfiguration, ? extends ApplicationContext> loadConfiguration(String springCfgPath)
        throws Exception {
        URL url;

        try {
            url = new URL(springCfgPath);
        }
        catch (MalformedURLException e) {
            url = IgniteUtils.resolveIgniteUrl(springCfgPath);

            if (url == null) {
                throw new IgniteCheckedException("Spring XML configuration path is invalid: " + springCfgPath +
                    ". Note that this path should be either absolute or a relative local file system path, " +
                    "relative to META-INF in classpath or valid URL to IGNITE_HOME.", e);
            }
        }

        GenericApplicationContext springCtx;

        try {
            springCtx = new GenericApplicationContext();

            new XmlBeanDefinitionReader(springCtx).loadBeanDefinitions(new UrlResource(url));

            springCtx.refresh();
        }
        catch (BeansException e) {
            throw new Exception("Failed to instantiate Spring XML application context [springUrl=" +
                url + ", err=" + e.getMessage() + ']', e);
        }

        Map<String, IgniteConfiguration> cfgMap;

        try {
            cfgMap = springCtx.getBeansOfType(IgniteConfiguration.class);
        }
        catch (BeansException e) {
            throw new Exception("Failed to instantiate bean [type=" + IgniteConfiguration.class + ", err=" +
                e.getMessage() + ']', e);
        }

        if (cfgMap == null || cfgMap.isEmpty())
            throw new Exception("Failed to find ignite configuration in: " + url);

        return new IgniteBiTuple<>(cfgMap.values().iterator().next(), springCtx);
    }

    /**
     * @return Thin client.
     */
    public IgniteClient client() {
        return client;
    }
}
