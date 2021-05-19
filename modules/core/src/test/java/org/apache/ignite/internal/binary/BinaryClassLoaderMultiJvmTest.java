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

package org.apache.ignite.internal.binary;

import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test class covering feature that allows to unmarshal binary object with custom classloader.
 */
public class BinaryClassLoaderMultiJvmTest extends GridCommonAbstractTest {
    /** Person class name. */
    private static final String PERSON_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.Person";

    /** Enum class name. */
    private static final String ENUM_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.Color";

    /** Organization class name. */
    private static final String ORGANIZATION_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.Organization";

    /** Address class name. */
    private static final String ADDRESS_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.Address";

    /** Enum values. */
    private static final String[] enumVals = {"GREY", "RED", "GREEN", "PURPLE", "LIGHTBLUE"};

    /** Client id. */
    public static final int CLIENT_ID = 1;

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setNetworkTimeout(10000)
            .setClientMode(getTestIgniteInstanceName(CLIENT_ID).equals(igniteInstanceName))
            .setCacheConfiguration(
                new CacheConfiguration("SomeCache")
                    .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                    .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC),
                new CacheConfiguration("SomeCacheEnum")
                    .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                    .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC),
                new CacheConfiguration("OrganizationCache")
                    .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                    .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));
    }

    /**
     * Checks that binary object deserialization works with custom classloader.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLoadClassFromBinary() throws Exception {
        ClassLoader testClsLdr = new GridTestExternalClassLoader(new URL[] {
            new URL(GridTestProperties.getProperty("p2p.uri.cls"))});

        WeakReference<ClassLoader> clsLdrRef = new WeakReference<>(testClsLdr);

        try {
            Ignite ign = startGrids(2);

            ign.compute(ign.cluster().forRemotes()).broadcast(new IgniteRunnable() {
                @Override public void run() {

                    Ignite client = Ignition.ignite(getTestIgniteInstanceName(CLIENT_ID));

                    ClassLoader clientCestClsLdr;

                    try {
                        clientCestClsLdr = new GridTestExternalClassLoader(new URL[] {
                            new URL(GridTestProperties.getProperty("p2p.uri.cls"))});

                        info("Client name: " + client.name());

                        loadItems(clientCestClsLdr, client);
                        loadOrganization(clientCestClsLdr, client);
                        loadEnumItems(clientCestClsLdr, client);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            });

            checkItems(testClsLdr, "SomeCache", PERSON_CLASS_NAME, ign);

            checkItems(testClsLdr, "SomeCacheEnum", ENUM_CLASS_NAME, ign);

            checkItems(testClsLdr, "OrganizationCache", ORGANIZATION_CLASS_NAME, ign);

            testClsLdr = null;

            checkClassCacheEmpty(clsLdrRef);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Checks that binary object deserialization works with custom classloader if called on client side.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientLoadClassFromBinary() throws Exception {
        ClassLoader testClsLdr = new GridTestExternalClassLoader(new URL[] {
            new URL(GridTestProperties.getProperty("p2p.uri.cls"))});

        WeakReference<ClassLoader> clsLdrRef = new WeakReference<>(testClsLdr);

        try {
            Ignite ign = startGrids(2);

            ign.compute(ign.cluster().forClients()).broadcast(new IgniteRunnable() {
                @Override public void run() {

                    Ignite client = Ignition.ignite(getTestIgniteInstanceName(CLIENT_ID));

                    ClassLoader clientCestClsLdr;

                    try {
                        clientCestClsLdr = new GridTestExternalClassLoader(new URL[] {
                            new URL(GridTestProperties.getProperty("p2p.uri.cls"))});

                        info("Client name: " + client.name());

                        loadItems(clientCestClsLdr, client);
                        loadOrganization(clientCestClsLdr, client);
                        loadEnumItems(clientCestClsLdr, client);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            checkItems(testClsLdr, "SomeCache", PERSON_CLASS_NAME, ign);

            checkItems(testClsLdr, "SomeCacheEnum", ENUM_CLASS_NAME, ign);

            checkItems(testClsLdr, "OrganizationCache", ORGANIZATION_CLASS_NAME, ign);

            testClsLdr = null;

            checkClassCacheEmpty(clsLdrRef);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param clsLdrRef Weak reference to testclassLoader.
     */
    private void checkClassCacheEmpty(WeakReference<ClassLoader> clsLdrRef) throws IgniteInterruptedCheckedException {
        System.gc();
        System.runFinalization();

        IgniteUtils.sleep(1_000);

        assertNull(clsLdrRef.get());
    }

    /**
     * @param testClsLdr Test class loader.
     * @param ignite Ignite.
     */
    private void checkItems(ClassLoader testClsLdr, String cacheName, String valClsName, Ignite ignite) {
        IgniteCache<Integer, Object> cache = ignite.cache(cacheName);

        IgniteCache<Integer, BinaryObject> binaryCache = cache.withKeepBinary();

        for (int i = 0; i < 100; i++) {

            BinaryObject binaryVal = binaryCache.get(i);

            if (i % 50 == 0)
                try {
                    info("Val: " + binaryVal.toString());
                }
                catch (IgniteException e) {
                    info("Can not execute toString() on class " + binaryVal.type().typeName());
                }

            assertEquals(binaryVal.type().typeName(), valClsName);

            boolean catchEx = false;

            try {
                binaryVal.deserialize();
            }
            catch (BinaryObjectException e) {
                ClassNotFoundException cause = X.cause(e, ClassNotFoundException.class);

                if (cause != null && cause.getMessage().contains(valClsName))
                    catchEx = true;
                else
                    throw e;
            }

            assertTrue(catchEx);

            Object personVal = binaryVal.deserialize(testClsLdr);

            assertTrue(personVal != null && personVal.getClass().getName().equals(valClsName));
        }
    }

    /**
     * @param testClsLdr Test class loader.
     * @param ignite Ignite.
     */
    private void loadItems(ClassLoader testClsLdr, Ignite ignite) throws Exception {
        Constructor personConstructor = testClsLdr.loadClass(PERSON_CLASS_NAME).getConstructor(String.class);

        IgniteCache<Integer, Object> cache = ignite.cache("SomeCache");

        for (int i = 0; i < 100; i++)
            cache.put(i, personConstructor.newInstance("Persone name " + i));

        assertEquals(cache.size(CachePeekMode.PRIMARY), 100);
    }

    /**
     * @param testClsLdr Test class loader.
     * @param ignite Ignite.
     */
    private void loadOrganization(ClassLoader testClsLdr, Ignite ignite) throws Exception {
        Class personCls = testClsLdr.loadClass(PERSON_CLASS_NAME);
        Class addrCls = testClsLdr.loadClass(ADDRESS_CLASS_NAME);

        Constructor personConstructor = testClsLdr.loadClass(PERSON_CLASS_NAME).getConstructor(String.class);
        Constructor addrConstructor = testClsLdr.loadClass(ADDRESS_CLASS_NAME).getConstructor(String.class, Integer.TYPE);
        Constructor organizationConstructor =
            testClsLdr.loadClass(ORGANIZATION_CLASS_NAME).getConstructor(String.class, personCls, addrCls);

        IgniteCache<Integer, Object> cache = ignite.cache("OrganizationCache");

        for (int i = 0; i < 100; i++)
            cache.put(i, organizationConstructor.newInstance("Organization " + i,
                personConstructor.newInstance("Persone name " + i),
                addrConstructor.newInstance("Street " + i, i)));

        assertEquals(cache.size(CachePeekMode.PRIMARY), 100);
    }

    /**
     * @param testClsLdr Test class loader.
     * @param ignite Ignite.
     */
    private void loadEnumItems(ClassLoader testClsLdr, Ignite ignite) throws Exception {
        Method factoryMtd = testClsLdr.loadClass(ENUM_CLASS_NAME).getMethod("valueOf", String.class);

        IgniteCache<Integer, Object> cache = ignite.cache("SomeCacheEnum");

        for (int i = 0; i < 100; i++)
            cache.put(i, factoryMtd.invoke(null, enumVals[i % enumVals.length]));

        assertEquals(cache.size(CachePeekMode.PRIMARY), 100);
    }

}
