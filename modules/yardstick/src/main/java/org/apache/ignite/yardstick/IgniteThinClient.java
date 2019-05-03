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
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
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

    /** */
    public IgniteClient start(BenchmarkConfiguration cfg, String host) throws Exception {
        IgniteBenchmarkArguments args = new IgniteBenchmarkArguments();

        BenchmarkUtils.jcommander(cfg.commandLineArguments(), args, "<ignite-node>");

        IgniteBiTuple<IgniteConfiguration, ? extends ApplicationContext> tup = loadConfiguration(args.configuration());

        IgniteConfiguration c = tup.get1();

        assert c != null;

        if (args.cleanWorkDirectory())
            FileUtils.cleanDirectory(U.workDirectory(c.getWorkDirectory(), c.getIgniteHome()));

        ClientConfiguration clCfg = new ClientConfiguration();

        String hostPort = host + ":10800";

        BenchmarkUtils.println(String.format("Using for connection address: %s", hostPort));

        clCfg.setAddresses(hostPort);

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
