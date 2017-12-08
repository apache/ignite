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

package org.apache.ignite.p2p;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridPeerDeployAware;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Test P2P deployment task which has internal field initialized with different class loader.
 */
public class GridP2PInternalClassLoaderTest extends GridCommonAbstractTest {
    /**
     * Internal class name.
     */
    private static final String FIELD_CLASS_NAME = "org.apache.ignite.tests.p2p.CacheDeploymentTestTask3";

    /**
     * URL of classes.
     */
    private static final URL[] URLS;

    /**
     * Current deployment mode. Used in {@link #getConfiguration(String)}.
     */
    private DeploymentMode depMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    static {
        try {
            URLS = new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))};
        }
        catch (MalformedURLException e) {
            throw new RuntimeException("Define property p2p.uri.cls", e);
        }
    }

    /**
     *
     * @throws Exception if error occur
     */
    private void processTest() throws Exception {

        try {
            Ignite ignite = startGrids(2);

            ClassLoader ldr = getExternalClassLoader();;

            Class<?> newClCls = ldr.loadClass(FIELD_CLASS_NAME);

            Constructor<?> newClCons = newClCls.getConstructor();

            Serializable newClInstance = (Serializable)newClCons.newInstance();

            ignite.compute(ignite.cluster().forRemotes()).call(new NewCallable(newClInstance, ldr));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testPrivateMode() throws Exception {
        depMode = DeploymentMode.PRIVATE;

        processTest();
    }

    /**
     * Test {@link org.apache.ignite.configuration.DeploymentMode#CONTINUOUS} mode.
     *
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        processTest();
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        depMode = DeploymentMode.SHARED;

        processTest();
    }

    /** */
    private static class NewCallable implements IgniteCallable, GridPeerDeployAware {

        /** */
        private final Serializable field;

        /** */
        private final ClassLoader classLdr;

        /** {@inheritDoc} */
        public NewCallable(Serializable f, ClassLoader cl) {
            this.field = f;

            this.classLdr = cl;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            System.out.println(field);

            return null;
        }

        /** {@inheritDoc} */
        @Override public Class<?> deployClass() {
            return this.getClass();
        }

        /** {@inheritDoc} */
        @Override public ClassLoader classLoader() {
            return this.classLdr;
        }
    }
}
