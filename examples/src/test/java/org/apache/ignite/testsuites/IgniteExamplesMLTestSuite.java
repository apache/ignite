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

package org.apache.ignite.testsuites;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

/**
 * Examples test suite.
 * <p>
 * Contains only ML Grid Ignite examples tests.</p>
 */
public class IgniteExamplesMLTestSuite {
    /** Base package to create test classes in. */
    private static final String basePkgForTests = "org.apache.ignite.examples.ml";

    /** Test class name pattern. */
    private static final String clsNamePtrn = ".*Example$";

    /** */
    @BeforeAll
    public static void init() {
        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP,
            GridTestUtils.getNextMulticastGroup(IgniteExamplesMLTestSuite.class));
    }

    /**
     * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
     *
     * @param pkgName The base package.
     * @return The classes.
     * @throws ClassNotFoundException If some classes not found.
     * @throws IOException If some resources unavailable.
     */
    static List<Class> getClasses(String pkgName) throws ClassNotFoundException, IOException {
        String path = pkgName.replace('.', '/');

        Enumeration<URL> resources = Thread.currentThread()
            .getContextClassLoader()
            .getResources(path);

        List<File> dirs = new ArrayList<>();
        while (resources.hasMoreElements())
            dirs.add(new File(resources.nextElement().getFile()));

        List<Class> classes = new ArrayList<>();
        for (File directory : dirs)
            classes.addAll(findClasses(directory, pkgName, clsNamePtrn));

        return classes;
    }

    /**
     * Recursive method used to find all classes in a given directory and sub-dirs.
     *
     * @param dir The base directory.
     * @param pkgName The package name for classes found inside the base directory.
     * @param clsNamePtrn Class name pattern.
     * @return The classes.
     * @throws ClassNotFoundException If class not found.
     */
    private static List<Class> findClasses(File dir, String pkgName, String clsNamePtrn) throws ClassNotFoundException {
        List<Class> classes = new ArrayList<>();

        if (!dir.exists())
            return classes;

        File[] files = dir.listFiles();
        if (files != null)
            for (File file : files) {
                if (file.isDirectory())
                    classes.addAll(findClasses(file, pkgName + "." + file.getName(), clsNamePtrn));
                else if (file.getName().endsWith(".class")) {
                    String clsName = pkgName + '.' + file.getName().substring(0, file.getName().length() - 6);

                    if (clsName.matches(clsNamePtrn))
                        classes.add(Class.forName(clsName));
                }
            }

        return classes;
    }

    @TestFactory
    Collection<DynamicTest> mlModule() throws IOException, ClassNotFoundException {
        List<Class> classes = getClasses(basePkgForTests);
        Collection<DynamicTest> classesList = new ArrayList<>();
        String[] params = {"--unattended"};

        for (Class<?> cl : classes)
            classesList.add(dynamicTest(cl.getSimpleName(),
                () -> cl.getMethod("main", String[].class).invoke(null, (Object)params)));

        return classesList;
    }
}
