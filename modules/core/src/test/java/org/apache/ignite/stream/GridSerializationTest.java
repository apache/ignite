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

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestClassLoader;
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

        return conf;
    }

    @SuppressWarnings("unchecked")
    public void testWrappedCallable() throws Exception {
        startGrid(0);
        try {
            IgniteEx client = startGrid(1);

            URL[] classpath = {new URL(GridTestProperties.getProperty("p2p.uri.cls"))};
            GridTestExternalClassLoader loader = new GridTestExternalClassLoader(classpath);

            Class<?> callableClass = loader.loadClass(REMOTE_CALLABLE_CLASS);

            Serializable instance = (Serializable) callableClass.newInstance();

            IgniteCallable<String> wrappedCallable = new WrappedCallable(instance);
            
            String result = ((IgniteProcessProxy) client).remoteCompute().call(wrappedCallable);
            Assert.assertEquals("here", result);
        } finally {
            stopAllGrids();
        }
    }

    @SuppressWarnings("unchecked")
    public void testWrappedCallable2() throws Exception {
        startGrid(0);
        try {
            IgniteEx client = startGrid(1);

            GridTestClassLoader loader = new GridTestClassLoader();

            Class<?> callableClass = loader.loadClass(REMOTE_CALLABLE_CLASS);

            Serializable instance = (Serializable) callableClass.newInstance();

            IgniteCallable<String> wrappedCallable = new WrappedCallable(instance);

            String result = ((IgniteProcessProxy) client).remoteCompute().call(wrappedCallable);
            Assert.assertEquals("here", result);
        } finally {
            stopAllGrids();
        }
    }

    @SuppressWarnings("unchecked")
    public void testClassCallable() throws Exception {
        startGrid(0);
        try {
            IgniteEx client = startGrid(1);

            URL[] classpath = {new URL(GridTestProperties.getProperty("p2p.uri.cls"))};
            GridTestExternalClassLoader loader = new GridTestExternalClassLoader(classpath);

            Class<?> callableClass = loader.loadClass(REMOTE_CALLABLE_CLASS);

            IgniteCallable<String> wrappedCallable = new ClassCallable(callableClass);

            String result = ((IgniteProcessProxy) client).remoteCompute().call(wrappedCallable);
            Assert.assertEquals("here", result);
        } finally {
            stopAllGrids();
        }
    }

    @SuppressWarnings("unchecked")
    public void testLoaderCallable() throws Exception {
        startGrid(0);
        try {
            IgniteEx client = startGrid(1);

            URL[] classpath = {new URL(GridTestProperties.getProperty("p2p.uri.cls"))};
            GridTestExternalClassLoader loader = new GridTestExternalClassLoader(classpath);

            IgniteCallable<String> wrappedCallable = new LoaderCallable(loader);

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

    private static class WrappedCallable implements IgniteCallable {

        private final Serializable callable;

        WrappedCallable(Serializable callable) {
            this.callable = callable;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object call() throws Exception {
            return ((IgniteCallable<?>) callable).call();
        }

    }

    private static class ClassCallable implements IgniteCallable {

        private final Class<?> callableClass;

        ClassCallable(Class<?> callableClass) {
            this.callableClass = callableClass;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object call() throws Exception {

            IgniteCallable<String> instance = (IgniteCallable<String>) callableClass.newInstance();
            System.out.println(instance.call());

            return instance.call();
        }
    }

    private static class LoaderCallable implements IgniteCallable {

        private final ClassLoader loader;

        LoaderCallable(ClassLoader loader) {
            this.loader = loader;
        }


        @Override
        @SuppressWarnings("unchecked")
        public Object call() throws Exception {

            System.out.println(loader);
            Class<?> callableClass = loader.loadClass(REMOTE_CALLABLE_CLASS);
            IgniteCallable<String> instance = (IgniteCallable<String>) callableClass.newInstance();
            System.out.println(instance.call());

            return instance.call();
        }

    }


}