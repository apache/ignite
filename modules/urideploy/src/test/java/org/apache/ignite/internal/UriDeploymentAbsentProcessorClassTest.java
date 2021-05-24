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

package org.apache.ignite.internal;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshallerInaccessibleClassException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.deployment.uri.UriDeploymentSpi;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class UriDeploymentAbsentProcessorClassTest extends GridCommonAbstractTest {
    /** */
    private static final String RUN_CLS = "org.apache.ignite.tests.p2p.compute.ExternalEntryProcessor";

    /** */
    private Path file;

    /** */
    private Object retVal;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        UriDeploymentSpi spi = new UriDeploymentSpi();

        file = Files.createTempDirectory(getClass().getName());

        spi.setUriList(Collections.singletonList(file.toUri().toString()));

        return super.getConfiguration(igniteInstanceName)
            .setPeerClassLoadingEnabled(false)
            .setDeploymentSpi(spi)
            .setDeploymentMode(DeploymentMode.SHARED);
    }

    /**
     * Starts server node with URI deployment of empty directory.
     * Starts client node.
     * In a separate thread, load entry processor from p2p class loader.
     * Try to invoke it on server node, catch expected exception.
     * Check that server node is operational after.
     *
     * @throws Exception if failed.
     */
    @Test
    public void test() throws Exception {
        try {
            startGrid(1);

            final Ignite ignite2 = startClientGrid(2);

            Thread thread = new Thread(() -> {
                try {
                    ClassLoader cl = new GridTestExternalClassLoader(new URL[] {
                        new URL(GridTestProperties.getProperty("p2p.uri.cls")) });

                    Thread.currentThread().setContextClassLoader(cl);

                    Class cls = cl.loadClass(RUN_CLS);

                    EntryProcessor proc = (EntryProcessor)cls.newInstance();

                    retVal = ignite2.getOrCreateCache(DEFAULT_CACHE_NAME).invoke(0, proc);
                }
                catch (Exception ex) {
                    if (X.hasCause(ex, "Failed to find class with given class loader for unmarshalling",
                        OptimizedMarshallerInaccessibleClassException.class)) {
                        ignite2.log().warning("Caught expected exception", ex);

                        retVal = 0;
                    }
                    else
                        throw new RuntimeException(ex);
                }
            });

            thread.start();

            thread.join();

            assertEquals(0, retVal);

            assertEquals(0, grid(1).cache(DEFAULT_CACHE_NAME).size());
        }
        finally {
            stopAllGrids();

            if (file != null)
                file.toFile().delete();
        }
    }
}
