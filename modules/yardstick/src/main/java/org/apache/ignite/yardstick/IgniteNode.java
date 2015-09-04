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
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.UrlResource;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkServer;
import org.yardstickframework.BenchmarkUtils;

import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_VALUES;

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

        IgniteConfiguration c = loadConfiguration(args.configuration());

        assert c != null;

        for (CacheConfiguration cc : c.getCacheConfiguration()) {
            // IgniteNode can not run in CLIENT_ONLY mode,
            // except the case when it's used inside IgniteAbstractBenchmark.
            boolean cl = args.isClientOnly() && !args.isNearCache() && !clientMode ?
                false : args.isClientOnly();

            if (cl)
                c.setClientMode(true);

            if (args.isNearCache())
                cc.setNearConfiguration(new NearCacheConfiguration());

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
        }

        TransactionConfiguration tc = c.getTransactionConfiguration();

        tc.setDefaultTxConcurrency(args.txConcurrency());
        tc.setDefaultTxIsolation(args.txIsolation());

        TcpCommunicationSpi commSpi = (TcpCommunicationSpi)c.getCommunicationSpi();

        if (commSpi == null)
            commSpi = new TcpCommunicationSpi();

        c.setCommunicationSpi(commSpi);

        ignite = Ignition.start(c);
    }

    /**
     * @param springCfgPath Spring configuration file path.
     * @return Grid configuration.
     * @throws Exception If failed.
     */
    private static IgniteConfiguration loadConfiguration(String springCfgPath) throws Exception {
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

        return cfgMap.values().iterator().next();
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