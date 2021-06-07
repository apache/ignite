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

package org.apache.ignite.app;

import java.util.ServiceLoader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Service loader based implementation of an entry point for handling grid lifecycle.
 */
public class IgnitionManager {
    /** Loaded Ignition instance. */
    private static Ignition ignition;

    /**
     * Starts Ignite node with optional bootstrap configuration in json format.
     *
     * @param nodeName Name of the node.
     * @param jsonStrBootstrapCfg Node configuration in json format.
     * @return Started Ignite node.
     */
    // TODO IGNITE-14580 Add exception handling logic to IgnitionProcessor.
    public static synchronized Ignite start(@NotNull String nodeName, @Nullable String jsonStrBootstrapCfg) {
        if (ignition == null) {
            ServiceLoader<Ignition> ldr = ServiceLoader.load(Ignition.class);
            ignition = ldr.iterator().next();
        }

        return ignition.start(nodeName, jsonStrBootstrapCfg);
    }

    /**
     * Starts Ignite node with optional bootstrap configuration in json format.
     * @param nodeName Name of the node.
     * @param jsonStrBootstrapCfg Node configuration in json format.
     * @param clsLdr The class loader to be used to load provider-configuration files
     * and provider classes, or {@code null} if the system class
     * loader (or, failing that, the bootstrap class loader) is to be used
     * @return Started Ignite node.
     */
    // TODO IGNITE-14580 Add exception handling logic to IgnitionProcessor.
    public static synchronized Ignite start(@NotNull String nodeName, @Nullable String jsonStrBootstrapCfg, @Nullable ClassLoader clsLdr) {
        if (ignition == null) {
            ServiceLoader<Ignition> ldr = ServiceLoader.load(Ignition.class, clsLdr);
            ignition = ldr.iterator().next();
        }

        return ignition.start(nodeName, jsonStrBootstrapCfg);
    }
}
