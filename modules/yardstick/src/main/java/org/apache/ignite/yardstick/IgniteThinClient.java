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

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSpring;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.yardstick.io.FileUtils;
import org.apache.ignite.yardstick.thin.cache.IgniteThinBenchmarkUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.UrlResource;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkServer;
import org.yardstickframework.BenchmarkUtils;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER;

/**
 * Standalone Ignite node.
 */
public class IgniteThinClient {
    /** Grid client. */
    private IgniteClient client;

    /** */
    public IgniteThinClient() {
        // No-op.
    }

    /**
     * @param ignite Use exist ignite client.
     */
    public IgniteThinClient(IgniteClient ignite) {
        this.client = ignite;
    }

    /** {@inheritDoc} */
    public IgniteClient start(BenchmarkConfiguration cfg) throws Exception {
        IgniteBenchmarkArguments args = new IgniteBenchmarkArguments();

        BenchmarkUtils.jcommander(cfg.commandLineArguments(), args, "<ignite-node>");

        IgniteBiTuple<IgniteConfiguration, ? extends ApplicationContext> tup = loadConfiguration(args.configuration());

        IgniteConfiguration c = tup.get1();

        assert c != null;

        if (args.cleanWorkDirectory())
            FileUtils.cleanDirectory(U.workDirectory(c.getWorkDirectory(), c.getIgniteHome()));

        ClientConfiguration clCfg = new ClientConfiguration();

        String[] servHostArr = IgniteThinBenchmarkUtils.servHostArr(cfg);

        String hostPort;

        String locIp = IgniteThinBenchmarkUtils.getLocalIp(cfg);

        int num = getNum(cfg, locIp);

        if(servHostArr.length == 1)
            hostPort = servHostArr[0] + ":10800";
        else{
            int idx = num % servHostArr.length;

            hostPort = servHostArr[idx] + ":10800";
        }

        BenchmarkUtils.println(String.format("Using for connection address + %s", hostPort));

        clCfg.setAddresses(hostPort);

        client = Ignition.startClient(clCfg);

        return client;
    }

    /**
     *
     * @param cfg
     * @param locIp
     * @return
     */
    private static int getNum(BenchmarkConfiguration cfg, String locIp){
        List<String> drvHosts = IgniteThinBenchmarkUtils.drvHostList(cfg);

        for(int i = 0; i<drvHosts.size(); i++){
            if(drvHosts.get(i).equals(locIp))
                return i;
        }

        return 0;
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

//    /** {@inheritDoc} */
//    @Override public void stop() throws Exception {
//        Ignition.stopAll(true);
//    }
//
//    /** {@inheritDoc} */
//    @Override public String usage() {
//        return BenchmarkUtils.usage(new IgniteBenchmarkArguments());
//    }

    /**
     * @return Ignite.
     */
    public IgniteClient client() {
        return client;
    }
}
