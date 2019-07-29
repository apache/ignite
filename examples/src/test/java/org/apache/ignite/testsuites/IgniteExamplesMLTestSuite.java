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

package org.apache.ignite.testsuites;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;
import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CodeConverter;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;
import javassist.bytecode.ConstPool;
import javassist.bytecode.annotation.Annotation;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ml.util.MLExamplesCommonArgs;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;

/**
 * Examples test suite.
 * <p>
 * Contains only ML Grid Ignite examples tests.</p>
 */
@RunWith(IgniteExamplesMLTestSuite.DynamicSuite.class)
public class IgniteExamplesMLTestSuite {
    /** Base package to create test classes in. */
    private static final String basePkgForTests = "org.apache.ignite.examples.ml";

    /** Test class name pattern. */
    private static final String clsNamePtrn = ".*Example$";

    /** Ignite proxy. */
    private static Ignite igniteProxy;

    /** Ignite for tests. */
    private static Ignite localIgnite;

    /** */
    @BeforeClass
    public static void init() {
        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP,
            GridTestUtils.getNextMulticastGroup(IgniteExamplesMLTestSuite.class));
    }

    /** */
    @AfterClass
    public static void tearDown() throws Exception {
        if (localIgnite != null)
            localIgnite.close();
    }

    /** */
    public static Ignite getTestIgnite(String someString) {
        if (igniteProxy == null) {
            localIgnite = Ignition.start("examples/config/example-ignite-ml.xml");
            InvocationHandler handler = new InvocationHandler() {
                @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    if (method.getName().equals("close"))
                        return 42;
                    else
                        return Ignite.class.getMethod(method.getName(), method.getParameterTypes()).invoke(localIgnite, args);
                }
            };

            igniteProxy = (Ignite) Proxy.newProxyInstance(Ignite.class.getClassLoader(), new Class[] {Ignite.class}, handler);
        }

        return igniteProxy;
    }

    /**
     * Creates list of test classes for Ignite ML examples.
     *
     * @return Created list.
     * @throws IOException, ClassNotFoundException If failed.
     */
    public static Class<?>[] suite() throws IOException, ClassNotFoundException {
        return getClasses(basePkgForTests)
            .stream()
            .map(IgniteExamplesMLTestSuite::makeTestClass)
            .collect(Collectors.toList())
            .toArray(new Class[] {null});
    }

    /**
     * Creates test class for given example.
     *
     * @param exampleCls Class of the example to be tested.
     * @return Test class.
     */
    private static Class<?> makeTestClass(Class<?> exampleCls) {
        ClassPool cp = ClassPool.getDefault();

        cp.insertClassPath(new ClassClassPath(IgniteExamplesMLTestSuite.class));

        CtClass cl = cp.makeClass(basePkgForTests + "." + exampleCls.getSimpleName() + "SelfTest");

        try {
            CtMethod mtd = CtNewMethod.make("public void testExample() { "
                + exampleCls.getCanonicalName()
                + ".main("
                + MLExamplesCommonArgs.class.getName()
                + ".EMPTY_ARGS_ML); }", cl);

            // Create and add annotation.
            ClassFile ccFile = cl.getClassFile();
            ConstPool constpool = ccFile.getConstPool();

            AnnotationsAttribute attr = new AnnotationsAttribute(constpool, AnnotationsAttribute.visibleTag);
            Annotation annot = new Annotation("org.junit.Test", constpool);

            attr.addAnnotation(annot);
            mtd.getMethodInfo().addAttribute(attr);

            cl.addMethod(mtd);

            return cl.toClass();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
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
        for (File directory : dirs) {
            // Replace Ignition.Start call in Tutorial.
            List<Class> tutorialClassess = findClassesAndReplaceIgniteStartCall(directory, pkgName, ".*Step_\\d+.*");
            A.notEmpty(tutorialClassess, "tutorialClassess.size() != 0");
            classes.addAll(findClassesAndReplaceIgniteStartCall(directory, pkgName, clsNamePtrn));
        }

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
    private static List<Class> findClassesAndReplaceIgniteStartCall(File dir, String pkgName,
        String clsNamePtrn) throws ClassNotFoundException {
        List<Class> classes = new ArrayList<>();

        if (!dir.exists())
            return classes;

        File[] files = dir.listFiles();
        if (files != null)
            for (File file : files) {
                if (file.isDirectory())
                    classes.addAll(findClassesAndReplaceIgniteStartCall(file, pkgName + "." + file.getName(), clsNamePtrn));
                else if (file.getName().endsWith(".class")) {
                    String clsName = pkgName + '.' + file.getName().substring(0, file.getName().length() - 6);

                    if (clsName.matches(clsNamePtrn))
                        classes.add(replaceIgniteStartCall(clsName));
                }
            }

        return classes;
    }

    /** */
    private static Class replaceIgniteStartCall(String clsName) {
        try {
            ClassPool cp = ClassPool.getDefault();
            cp.insertClassPath(new ClassClassPath(Ignition.class));
            cp.insertClassPath(new ClassClassPath(IgniteExamplesMLTestSuite.class));

            CtClass ignClass = cp.get(Ignition.class.getName());
            CtClass suiteClass = cp.get(IgniteExamplesMLTestSuite.class.getName());
            CtMethod getTestIgniteMethod = suiteClass.getDeclaredMethod("getTestIgnite");

            CtMethod startM = null;
            for (CtMethod m : ignClass.getDeclaredMethods("start")) {
                CtClass[] params = m.getParameterTypes();
                if (params.length == 1 && params[0].getName().equals(String.class.getName())) {
                    startM = m;
                    break;
                }
            }

            CodeConverter converter = new CodeConverter();
            converter.redirectMethodCall(startM, getTestIgniteMethod);
            cp.get(clsName).instrument(converter);
            return cp.get(clsName).toClass();
        }
        catch (NotFoundException | CannotCompileException e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    public static class DynamicSuite extends Suite {

        /** */
        public DynamicSuite(Class<?> cls) throws InitializationError, IOException, ClassNotFoundException {
            super(cls, suite());
        }
    }
}
