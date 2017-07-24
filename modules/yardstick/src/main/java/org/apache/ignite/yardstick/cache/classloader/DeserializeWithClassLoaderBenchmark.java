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

package org.apache.ignite.yardstick.cache.classloader;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.jetbrains.annotations.NotNull;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 */
public class DeserializeWithClassLoaderBenchmark extends IgniteAbstractBenchmark {

    private static final String PERSON_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.Person";
    private static final String ENUM_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.Color";
    private static final String ORGANIZATION_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.Organization";
    private static final String ADDRESS_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.Address";

    /** Value classes. */
    private String[] valClasses = {
        PERSON_CLASS_NAME,
        ENUM_CLASS_NAME,
        ORGANIZATION_CLASS_NAME,
//        ADDRESS_CLASS_NAME
    };

    String jarPath = "file:\\D:\\Work\\incubator-ignite\\modules\\extdata\\p2p\\target\\ignite-extdata-p2p-2.1.0-SNAPSHOT.jar";
    ClassLoader testClassLoader;
    boolean useCache;

    /** List off all available cache. */
    private List<IgniteCache<Object, Object>> availableCaches;

    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        availableCaches = new ArrayList<>(ignite().cacheNames().size());

        for (String cacheName : ignite().cacheNames()) {
            IgniteCache<Object, Object> cache = ignite().cache(cacheName);

            availableCaches.add(cache);
        }

        testClassLoader = new URLClassLoader(new URL[] {new URL(jarPath)});
        useCache = args.isNearCache();

        BenchmarkUtils.println("Use cache fals: " + useCache);

        preLoading();

//        ClassLoader testClassLoader = new GridTestExternalClassLoader(new URL[]{
//            new URL(GridTestProperties.getProperty("p2p.uri.cls"))});

    }

    /**
     * @throws Exception If fail.
     */
    private void preLoading() throws Exception {
        startPreloadLogging(args.preloadLogsInterval());

        ExecutorService executor = Executors.newFixedThreadPool(10);

        try {
            List<Future<?>> futs = new ArrayList<>();

            final Thread thread = Thread.currentThread();

            for (int i = 0; i < availableCaches.size(); i++) {
                final String cacheName = availableCaches.get(i).getName();

                futs.add(executor.submit(new Runnable() {
                    @Override public void run() {
                        try (IgniteDataStreamer<Object, Object> dataLdr = ignite().dataStreamer(cacheName)) {
                            for (int i = 0; i < args.range(); i++) {
                                if (i % 100 == 0 && thread.isInterrupted())
                                    break;

                                try {
                                    dataLdr.addData(i, createValueObject(i));
                                }
                                catch (Exception e) {
                                    BenchmarkUtils.error("Error on preloding.", e);
                                }
                            }
                        }
                    }
                }));
            }

            for (Future<?> fut : futs)
                fut.get();
        }
        finally {
            executor.shutdown();
        }

        stopPreloadLogging();
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {

        int key = nextRandom(args.range());

        for (IgniteCache cache : availableCaches) {
            BinaryObject bo = (BinaryObject)cache.withKeepBinary().get(key);

            bo.deserialize(testClassLoader, useCache);
        }

        return true;
    }

    /**
     * @param id Id.
     */
    private Object createValueObject(
        int id) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        String clsName = valClasses[id % valClasses.length];
        Class objCls = testClassLoader.loadClass(clsName);

        Object res;

        switch (clsName) {
            case PERSON_CLASS_NAME:
                res = createPerson(id, objCls);
                break;
            case ENUM_CLASS_NAME:
                res = createColor(id, objCls);
                break;
            case ADDRESS_CLASS_NAME:
                res = createAddress(id, objCls);
                break;
            case ORGANIZATION_CLASS_NAME:
                res = createOrganization(id, objCls);
                break;
            default:
                res = null;
        }

        return res;
    }

    /**
     * @param id Id.
     * @param objCls Object class.
     */
    @NotNull private Object createOrganization(long id, Class objCls)
        throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Class personCls = testClassLoader.loadClass(PERSON_CLASS_NAME);
        Class addrCls = testClassLoader.loadClass(ADDRESS_CLASS_NAME);

        Constructor organizationConstructor = objCls.getConstructor(String.class, personCls, addrCls);

        Object res = organizationConstructor.newInstance("Organization " + id, createPerson(id, personCls), createAddress(id, addrCls));

        return res;
    }

    /**
     * @param id Id.
     * @param objCls Object class.
     */
    @NotNull private Object createAddress(long id, Class objCls)
        throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Constructor addressConstructor = objCls.getConstructor(String.class, Integer.TYPE);

        Object res = addressConstructor.newInstance("Street " + id, (int)id);

        return res;
    }

    /**
     * @param id Id.
     * @param objCls Object class.
     */
    private Object createColor(long id, Class objCls)
        throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        Method factoryMtd = objCls.getMethod("valueOf", String.class);

        Object[] objects = objCls.getEnumConstants();

        Object res = factoryMtd.invoke(null, String.valueOf(objects[(int)(id % objects.length)]));

        return res;
    }

    /**
     * @param id Id.
     * @param objCls Object class.
     */
    @NotNull private Object createPerson(long id, Class objCls)
        throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {

        Constructor ctor = objCls.getConstructor(String.class);

        Object res = ctor.newInstance("Person name " + id);

        return res;
    }
}
