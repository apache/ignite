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

package org.apache.ignite.internal.commandline.cache;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.cache.VisorCacheCreateTask;

import static org.apache.ignite.internal.IgniteComponentType.SPRING;

/**
 * Command to create caches from Spring XML configuration.
 */
public class CacheCreate extends AbstractCommand<String> {
    /** */
    public static final String SPRING_XML_CONFIG = "--springXmlConfig";

    /** */
    private String springXmlCfgPath;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        if (!new File(springXmlCfgPath).exists()) {
            throw new IgniteException("Failed to create caches. Spring XML configuration file not found " +
                "[file=" + springXmlCfgPath + ']');
        }

        String springXmlCfg;

        try {
            springXmlCfg = U.readFileToString(springXmlCfgPath, "UTF-8");
        }
        catch (IOException e) {
            throw new IgniteException("Failed to create caches. Failed to read Spring XML configuration file " +
                "[file=" + springXmlCfgPath + ']', e);
        }

        try (GridClient client = Command.startClient(clientCfg)) {
            Set<String> caches = TaskExecutor.executeTask(client, VisorCacheCreateTask.class, springXmlCfg, clientCfg);

            GridStringBuilder s = new GridStringBuilder("Caches created successfully:").nl();

            for (String cache : caches)
                s.a(cache).nl();

            s.nl().a("Total count: " + caches.size());

            log.info(s.toString());

            return caches;
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        springXmlCfgPath = null;

        while (argIter.hasNextSubArg()) {
            String opt = argIter.nextArg("Failed to read command argument.");

            if (SPRING_XML_CONFIG.equalsIgnoreCase(opt)) {
                if (springXmlCfgPath != null)
                    throw new IllegalArgumentException(SPRING_XML_CONFIG + " argument specified twice.");

                if (!argIter.hasNextSubArg())
                    throw new IllegalArgumentException("Expected path to the Spring XML configuration.");

                springXmlCfgPath = argIter.nextArg("Expected path to the Spring XML configuration.");
            }
        }

        if (F.isEmpty(springXmlCfgPath))
            throw new IllegalArgumentException(SPRING_XML_CONFIG + " must be specified.");
    }

    /** {@inheritDoc} */
    @Override public String arg() {
        return springXmlCfgPath;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger log) {
        String springXmlPathArgDesc = SPRING_XML_CONFIG + " springXmlConfigPath";

        usageCache(log, CacheSubcommands.CREATE, "Create caches from Spring XML configuration. Note that the '" +
                SPRING.module() + "' module should be enabled.",
            F.asMap(springXmlPathArgDesc, "Path to the Spring XML configuration that contains '" +
                CacheConfiguration.class.getName() + "' beans to create caches from."),
            springXmlPathArgDesc);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CacheSubcommands.CREATE.name().toUpperCase();
    }
}
