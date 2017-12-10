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

package org.apache.ignite.stream;

import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.GridPeerDeployAware;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.Assert;

import java.io.Serializable;
import java.net.URL;


public class GridSerializationTest extends GridCommonAbstractTest {

    private static final String REMOTE_CALLABLE_CLASS = "org.apache.ignite.stream.callable.GridSimpleCallable";

    @Override
    protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration conf = super.getConfiguration(name);
        conf.setPeerClassLoadingEnabled(true);
        conf.setClientMode(isRemoteJvm());
        conf.setDeploymentMode(DeploymentMode.CONTINUOUS);

        return conf;
    }

    @SuppressWarnings("unchecked")
    public void testNewCallable() throws Exception {
        startGrid(0);
        try {
            IgniteEx client = startGrid(1);

            URL[] classpath = {new URL(GridTestProperties.getProperty("p2p.uri.cls"))};
            GridTestExternalClassLoader loader = new GridTestExternalClassLoader(classpath);

            Class<?> callableClass = loader.loadClass(REMOTE_CALLABLE_CLASS);

            Serializable instance = (Serializable) callableClass.newInstance();

            IgniteCallable<String> wrappedCallable = new WrappedCallable(instance, loader);

            String result = ((IgniteProcessProxy) client).remoteCompute().call(wrappedCallable);
            Assert.assertEquals("here", result);
        } finally {
            stopAllGrids();
        }
    }

    @Override
    protected boolean isMultiJvm() {
        return true;
    }

    private static class WrappedCallable implements IgniteCallable, GridPeerDeployAware {

        private final Serializable field;

        private final ClassLoader classLdr;

        public WrappedCallable(Serializable f, ClassLoader cl) {
            this.field = f;

            this.classLdr = cl;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object call() throws Exception {
            System.out.println(field);

            return ((IgniteCallable<String>) field).call();
        }

        @Override
        public Class<?> deployClass() {
            return this.getClass();
        }

        @Override
        public ClassLoader classLoader() {
            return this.classLdr;
        }

    }

}