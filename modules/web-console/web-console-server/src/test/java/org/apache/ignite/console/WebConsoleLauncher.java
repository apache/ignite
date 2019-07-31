/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console;

import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

/**
 * Web Console Launcher.
 */
public class WebConsoleLauncher {
    /**
     * Main entry point.
     *
     * @param args Arguments.
     */
    public static void main(String... args) throws IgniteCheckedException {
        System.out.println("Starting persistence node for Ignite Web Console Server...");

        String springCfgPath = "ignite-config.xml";

        URL cfgLocation = U.resolveSpringUrl(springCfgPath);

        ApplicationContext springCtx;

        try {
            springCtx = new FileSystemXmlApplicationContext(cfgLocation.toString());
        }
        catch (BeansException e) {
            throw new IgniteCheckedException("Failed to instantiate Spring XML application context.", e);
        }

        Map cfgMap;

        try {
            // Note: Spring is not generics-friendly.
            cfgMap = springCtx.getBeansOfType(IgniteConfiguration.class);
        }
        catch (BeansException e) {
            throw new IgniteCheckedException("Failed to instantiate bean [type=" + IgniteConfiguration.class + ", err=" +
                e.getMessage() + ']', e);
        }

        if (cfgMap == null)
            throw new IgniteCheckedException("Failed to find a single grid factory configuration in: " + springCfgPath);

        if (cfgMap.isEmpty())
            throw new IgniteCheckedException("Can't find grid factory configuration in: " + springCfgPath);
        else if (cfgMap.size() > 1)
            throw new IgniteCheckedException("More than one configuration provided for cache get test: " + cfgMap.values());

        IgniteConfiguration cfg = (IgniteConfiguration)cfgMap.values().iterator().next();

        String workDir = Paths.get(U.defaultWorkDirectory(), "ignite-node").toString();

        Ignite ignite = Ignition.getOrStart(
            cfg.setIgniteInstanceName("Web Console backend")
                .setClientMode(false)
                .setWorkDirectory(workDir)
        );

        ignite.cluster().active(true);
    }
}
