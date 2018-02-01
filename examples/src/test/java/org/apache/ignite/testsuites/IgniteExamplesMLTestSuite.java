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
import java.util.Enumeration;
import java.util.List;
import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import junit.framework.TestSuite;
import org.apache.ignite.examples.ml.MLExamplesCommonArgs;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;

/**
 * Examples test suite.
 * <p>
 * Contains only ML Grid Ignite examples tests.</p>
 */
public class IgniteExamplesMLTestSuite extends TestSuite {
    /** Base package to create test classes in. */
    private static final String basePkgForTests = "org.apache.ignite.examples.ml";

    /** Test class name pattern. */
    private static final String clsNamePtrn = ".*Example$";

    /**
     * Creates test suite for Ignite ML examples.
     *
     * @return Created suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP,
            GridTestUtils.getNextMulticastGroup(IgniteExamplesMLTestSuite.class));

        TestSuite suite = new TestSuite("Ignite ML Examples Test Suite");

        for (Class clazz : getClasses(basePkgForTests))
            suite.addTest(new TestSuite(makeTestClass(clazz)));

        return suite;
    }

    /**
     * Creates test class for given example.
     *
     * @param exampleCls Class of the example to be tested.
     * @return Test class.
     * @throws NotFoundException If class not found.
     * @throws CannotCompileException If test class cannot be compiled.
     */
    private static Class makeTestClass(Class<?> exampleCls)
        throws NotFoundException, CannotCompileException {
        ClassPool cp = ClassPool.getDefault();
        cp.insertClassPath(new ClassClassPath(IgniteExamplesMLTestSuite.class));

        CtClass cl = cp.makeClass(basePkgForTests + "." + exampleCls.getSimpleName() + "SelfName");

        cl.setSuperclass(cp.get(GridAbstractExamplesTest.class.getName()));

        cl.addMethod(CtNewMethod.make("public void testExample() { "
            + exampleCls.getCanonicalName()
            + ".main("
            + MLExamplesCommonArgs.class.getName()
            + ".EMPTY_ARGS_ML); }", cl));

        return cl.toClass();
    }

    /**
     * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
     *
     * @param pkgName The base package.
     * @return The classes.
     * @throws ClassNotFoundException If some classes not found.
     * @throws IOException If some resources unavailable.
     */
    private static List<Class> getClasses(String pkgName) throws ClassNotFoundException, IOException {
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
}
