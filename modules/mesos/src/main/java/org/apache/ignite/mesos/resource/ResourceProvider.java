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

package org.apache.ignite.mesos.resource;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.mesos.ClusterProperties;

import static org.apache.ignite.mesos.resource.ResourceHandler.CONFIG_PREFIX;
import static org.apache.ignite.mesos.resource.ResourceHandler.DEFAULT_CONFIG;
import static org.apache.ignite.mesos.resource.ResourceHandler.IGNITE_PREFIX;
import static org.apache.ignite.mesos.resource.ResourceHandler.LIBS_PREFIX;

/**
 * Provides path to user's libs and config file.
 */
public class ResourceProvider {
    /** Ignite url. */
    private String igniteUrl;

    /** Resources. */
    private Collection<String> libsUris;

    /** Url config. */
    private String configUrl;

    /** Config name. */
    private String configName;

    /**
     * @param properties Cluster properties.
     * @param provider Ignite provider.
     * @param baseUrl Base url.
     */
    public void init(ClusterProperties properties, IgniteProvider provider, String baseUrl) {
        // Downloading ignite.
        if (properties.igniteVer().equals(ClusterProperties.DEFAULT_IGNITE_VERSION))
            igniteUrl = baseUrl + IGNITE_PREFIX + provider.getIgnite();
        else
            igniteUrl = baseUrl + IGNITE_PREFIX + provider.getIgnite(properties.igniteVer());

        // Find all jar files into user folder.
        if (properties.userLibs() != null && !properties.userLibs().isEmpty()) {
            File libsDir = new File(properties.userLibs());

            List<String> libs = new ArrayList<>();

            if (libsDir.isDirectory()) {
                File[] files = libsDir.listFiles();

                if (files != null) {
                    for (File lib : files) {
                        if (lib.isFile() && lib.canRead() &&
                            (lib.getName().endsWith(".jar") || lib.getName().endsWith(".JAR")))
                            libs.add(baseUrl + LIBS_PREFIX + lib.getName());
                    }
                }
            }

            libsUris = libs.isEmpty() ? null : libs;
        }

        // Set configuration url.
        if (properties.igniteCfg() != null) {
            File cfg = new File(properties.igniteCfg());

            if (cfg.isFile() && cfg.canRead()) {
                configUrl = baseUrl + CONFIG_PREFIX + cfg.getName();

                configName = cfg.getName();
            }
        }
        else {
            configName = "ignite-default-config.xml";

            configUrl = baseUrl + DEFAULT_CONFIG + configName;
        }
    }

    /**
     * @return Config name.
     */
    public String configName() {
        return configName;
    }

    /**
     * @return Ignite url.
     */
    public String igniteUrl() {
        return igniteUrl;
    }

    /**
     * @return Urls to user's libs.
     */
    public Collection<String> resourceUrl() {
        return libsUris;
    }

    /**
     * @return Url to config file.
     */
    public String igniteConfigUrl() {
        return configUrl;
    }
}