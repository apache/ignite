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

package org.apache.ignite.internal.app;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.utils.IgniteProperties;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of an entry point for handling grid lifecycle.
 */
public class IgnitionImpl implements Ignition {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(IgnitionImpl.class);

    /**
     *
     */
    private static final String[] BANNER = {
            "",
            "           #              ___                         __",
            "         ###             /   |   ____   ____ _ _____ / /_   ___",
            "     #  #####           / /| |  / __ \\ / __ `// ___// __ \\ / _ \\",
            "   ###  ######         / ___ | / /_/ // /_/ // /__ / / / // ___/",
            "  #####  #######      /_/  |_|/ .___/ \\__,_/ \\___//_/ /_/ \\___/",
            "  #######  ######            /_/",
            "    ########  ####        ____               _  __           _____",
            "   #  ########  ##       /  _/____ _ ____   (_)/ /_ ___     |__  /",
            "  ####  #######  #       / / / __ `// __ \\ / // __// _ \\     /_ <",
            "   #####  #####        _/ / / /_/ // / / // // /_ / ___/   ___/ /",
            "     ####  ##         /___/ \\__, //_/ /_//_/ \\__/ \\___/   /____/",
            "       ##                  /____/\n"
    };

    /**
     *
     */
    private static final String VER_KEY = "version";

    /**
     * Node name to node instance mapping. Please pay attention, that nodes in given map might be in any state: STARTING, STARTED, STOPPED.
     */
    private static Map<String, IgniteImpl> nodes = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override
    public Ignite start(@NotNull String nodeName, @Nullable Path cfgPath, @NotNull Path workDir) {
        try {
            return doStart(
                    nodeName,
                    cfgPath == null ? null : Files.readString(cfgPath),
                    workDir
            );
        } catch (IOException e) {
            LOG.warn("Unable to read user specific configuration, default configuration will be used: "
                    + e.getMessage());
            return start(nodeName, workDir);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Ignite start(@NotNull String name, @Nullable InputStream cfg, @NotNull Path workDir) {
        try {
            return doStart(
                    name,
                    cfg == null ? null : new String(cfg.readAllBytes(), StandardCharsets.UTF_8),
                    workDir
            );
        } catch (IOException e) {
            LOG.warn("Unable to read user specific configuration, default configuration will be used: "
                    + e.getMessage());
            return start(name, workDir);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Ignite start(@NotNull String name, @NotNull Path workDir) {
        return doStart(name, null, workDir);
    }

    /** {@inheritDoc} */
    @Override
    public void stop(@NotNull String name) {
        nodes.computeIfPresent(name, (nodeName, node) -> {
            node.stop();

            return null;
        });
    }

    /**
     * Starts an Ignite node with an optional bootstrap configuration from a HOCON file.
     *
     * @param nodeName   Name of the node. Must not be {@code null}.
     * @param cfgContent Node configuration in the HOCON format. Can be {@code null}.
     * @param workDir    Work directory for the started node. Must not be {@code null}.
     * @return Started Ignite node.
     */
    private static Ignite doStart(String nodeName, @Nullable String cfgContent, Path workDir) {
        if (nodeName.isEmpty()) {
            throw new IllegalArgumentException("Node name must not be null or empty.");
        }

        IgniteImpl nodeToStart = new IgniteImpl(nodeName, workDir);

        IgniteImpl prevNode = nodes.putIfAbsent(nodeName, nodeToStart);

        if (prevNode != null) {
            String errMsg = "Node with name=[" + nodeName + "] already exists.";

            LOG.error(errMsg);

            throw new IgniteException(errMsg);
        }

        ackBanner();

        try {
            nodeToStart.start(cfgContent);
        } catch (Exception e) {
            nodes.remove(nodeName);

            throw new IgniteException(e);
        }

        ackSuccessStart();

        return nodeToStart;
    }

    /**
     *
     */
    private static void ackSuccessStart() {
        LOG.info("Apache Ignite started successfully!");
    }

    /**
     *
     */
    private static void ackBanner() {
        String ver = IgniteProperties.get(VER_KEY);

        String banner = String.join("\n", BANNER);

        LOG.info(() ->
                        LoggerMessageHelper.format("{}\n" + " ".repeat(22) + "Apache Ignite ver. {}\n", banner, ver),
                null);
    }
}
