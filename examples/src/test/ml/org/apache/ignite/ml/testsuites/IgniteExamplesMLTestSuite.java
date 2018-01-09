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

package org.apache.ignite.ml.testsuites;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import junit.framework.TestSuite;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;

/**
 * Examples test suite.
 * <p>
 * Contains only Spring ignite examples tests.
 */
public class IgniteExamplesMLTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP,
            GridTestUtils.getNextMulticastGroup(IgniteExamplesMLTestSuite.class));

        TestSuite suite = new TestSuite("Ignite ML Examples Test Suite");

        for (Class clazz : getClasses("org.apache.ignite.examples.ml", ".*Example$"))
            suite.addTest(new TestSuite(makeTestClass(clazz, "org.apache.ignite.ml.examples")));

        return suite;
    }

    /**
     * Creates test class for given example.
     *
     * @param exampleCls Class of the example to be tested
     * @param basePkgForTests Base package to create test classes in
     * @return Test class
     * @throws NotFoundException if class not found
     * @throws CannotCompileException if test class cannot be compiled
     */
    private static Class makeTestClass(Class<?> exampleCls, String basePkgForTests)
        throws NotFoundException, CannotCompileException {
        ClassPool cp = ClassPool.getDefault();

        CtClass cl = cp.makeClass(basePkgForTests + "." + exampleCls.getSimpleName() + "SelfName");

        cl.setSuperclass(cp.getCtClass(GridAbstractExamplesTest.class.getName()));

        cl.addMethod(CtNewMethod.make("public void testExample() { "
            + exampleCls.getCanonicalName()
            + ".main("
            + GridAbstractExamplesTest.class.getName()
            + ".EMPTY_ARGS); }", cl));

        return cl.toClass();
    }

    /**
     * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
     *
     * @param pkgName The base package
     * @return The classes
     * @throws ClassNotFoundException if some classes not found
     * @throws IOException if some resources unavailable
     */
    private static List<Class> getClasses(String pkgName, String clsNamePtrn) throws ClassNotFoundException, IOException {
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
     * Recursive method used to find all classes in a given directory and subdirs.
     *
     * @param dir The base directory
     * @param pkgName The package name for classes found inside the base directory
     * @return The classes
     * @throws ClassNotFoundException if class not found
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
}
