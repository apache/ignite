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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSpring;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.UrlResource;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkServer;
import org.yardstickframework.BenchmarkUtils;

import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_VALUES;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER;

/**
 * Standalone Ignite node.
 */
public class IgniteNode implements BenchmarkServer {
    /** Grid instance. */
    private Ignite ignite;

    /** Client mode. */
    private boolean clientMode;

    /** */
    public IgniteNode() {
        // No-op.
    }

    /** */
    public IgniteNode(boolean clientMode) {
        this.clientMode = clientMode;
    }

    /** */
    public IgniteNode(boolean clientMode, Ignite ignite) {
        this.clientMode = clientMode;
        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override public void start(BenchmarkConfiguration cfg) throws Exception {
        IgniteBenchmarkArguments args = new IgniteBenchmarkArguments();

        BenchmarkUtils.jcommander(cfg.commandLineArguments(), args, "<ignite-node>");

        IgniteBiTuple<IgniteConfiguration, ? extends ApplicationContext> tup = loadConfiguration(args.configuration());

        IgniteConfiguration c = tup.get1();

        assert c != null;

        ApplicationContext appCtx = tup.get2();

        assert appCtx != null;

        CacheConfiguration[] ccfgs = c.getCacheConfiguration();

        if (ccfgs != null) {
            for (CacheConfiguration cc : ccfgs) {
                // IgniteNode can not run in CLIENT_ONLY mode,
                // except the case when it's used inside IgniteAbstractBenchmark.
                boolean cl = args.isClientOnly() && (args.isNearCache() || clientMode);

                if (cl)
                    c.setClientMode(true);

                if (args.isNearCache()) {
                    NearCacheConfiguration nearCfg = new NearCacheConfiguration();

                    if (args.getNearCacheSize() != 0)
                        nearCfg.setNearEvictionPolicy(new LruEvictionPolicy(args.getNearCacheSize()));

                    cc.setNearConfiguration(nearCfg);
                }

                cc.setWriteSynchronizationMode(args.syncMode());

                if (args.orderMode() != null)
                    cc.setAtomicWriteOrderMode(args.orderMode());

                cc.setBackups(args.backups());

                if (args.restTcpPort() != 0) {
                    ConnectorConfiguration ccc = new ConnectorConfiguration();

                    ccc.setPort(args.restTcpPort());

                    if (args.restTcpHost() != null)
                        ccc.setHost(args.restTcpHost());

                    c.setConnectorConfiguration(ccc);
                }

                if (args.isOffHeap()) {
                    cc.setOffHeapMaxMemory(0);

                    if (args.isOffheapValues())
                        cc.setMemoryMode(OFFHEAP_VALUES);
                    else
                        cc.setEvictionPolicy(new LruEvictionPolicy(50000));
                }

                cc.setReadThrough(args.isStoreEnabled());

                cc.setWriteThrough(args.isStoreEnabled());

                cc.setWriteBehindEnabled(args.isWriteBehind());

                BenchmarkUtils.println(cfg, "Cache configured with the following parameters: " + cc);
            }
        }
        else
            BenchmarkUtils.println(cfg, "There are no caches configured");

        TransactionConfiguration tc = c.getTransactionConfiguration();

        tc.setDefaultTxConcurrency(args.txConcurrency());
        tc.setDefaultTxIsolation(args.txIsolation());

        TcpCommunicationSpi commSpi = (TcpCommunicationSpi)c.getCommunicationSpi();

        if (commSpi == null)
            commSpi = new TcpCommunicationSpi();

        c.setCommunicationSpi(commSpi);

        ignite = IgniteSpring.start(c, appCtx);

        BenchmarkUtils.println("Configured marshaller: " + ignite.cluster().localNode().attribute(ATTR_MARSHALLER));
    }

    /**
     * @param springCfgPath Spring configuration file path.
     * @return Tuple with grid configuration and Spring application context.
     * @throws Exception If failed.
     */
    public static IgniteBiTuple<IgniteConfiguration, ? extends ApplicationContext> loadConfiguration(String springCfgPath)
        throws Exception {
        URL url;

        try {
            url = new URL(springCfgPath);
        }
        catch (MalformedURLException e) {
            url = IgniteUtils.resolveIgniteUrl(springCfgPath);

            if (url == null)
                throw new IgniteCheckedException("Spring XML configuration path is invalid: " + springCfgPath +
                    ". Note that this path should be either absolute or a relative local file system path, " +
                    "relative to META-INF in classpath or valid URL to IGNITE_HOME.", e);
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

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        Ignition.stopAll(true);
    }

    /** {@inheritDoc} */
    @Override public String usage() {
        return BenchmarkUtils.usage(new IgniteBenchmarkArguments());
    }

    /**
     * @return Ignite.
     */
    public Ignite ignite() {
        return ignite;
    }
}
