/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.ArrayList;
import org.h2.test.TestBase;
import org.h2.util.New;

/**
 * Test that static references within the database engine don't reference the
 * class itself. For example, there is a leak if a class contains a static
 * reference to a stack trace. This was the case using the following
 * declaration: static EOFException EOF = new EOFException(). The way to solve
 * the problem is to not use such references, or to not fill in the stack trace
 * (which indirectly references the class loader).
 *
 * @author Erik Karlsson
 * @author Thomas Mueller
 */
public class TestClassLoaderLeak extends TestBase {

    /**
     * The name of this class (used by reflection).
     */
    static final String CLASS_NAME = TestClassLoaderLeak.class.getName();

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        WeakReference<ClassLoader> ref = createClassLoader();
        for (int i = 0; i < 10; i++) {
            System.gc();
            Thread.sleep(10);
        }
        ClassLoader cl = ref.get();
        assertTrue(cl == null);
        // fill the memory, so a heap dump is created
        // using -XX:+HeapDumpOnOutOfMemoryError
        // which can be analyzed using EclipseMAT
        // (check incoming references to TestClassLoader)
        boolean fillMemory = false;
        if (fillMemory) {
            ArrayList<byte[]> memory = New.arrayList();
            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                memory.add(new byte[1024]);
            }
        }
        DriverManager.registerDriver((Driver)
                Class.forName("org.h2.Driver").newInstance());
        DriverManager.registerDriver((Driver)
                Class.forName("org.h2.upgrade.v1_1.Driver").newInstance());
    }

    private static WeakReference<ClassLoader> createClassLoader() throws Exception {
        ClassLoader cl = new TestClassLoader();
        Class<?> h2ConnectionTestClass = Class.forName(CLASS_NAME, true, cl);
        Method testMethod = h2ConnectionTestClass.getDeclaredMethod("runTest");
        testMethod.setAccessible(true);
        testMethod.invoke(null);
        return new WeakReference<>(cl);
    }

    /**
     * This method is called using reflection.
     */
    static void runTest() throws Exception {
        Class.forName("org.h2.Driver");
        Class.forName("org.h2.upgrade.v1_1.Driver");
        Driver d1 = DriverManager.getDriver("jdbc:h2:mem:test");
        Driver d2 = DriverManager.getDriver("jdbc:h2v1_1:mem:test");
        Connection connection;
        connection = DriverManager.getConnection("jdbc:h2:mem:test");
        DriverManager.deregisterDriver(d1);
        DriverManager.deregisterDriver(d2);
        connection.close();
        connection = null;
    }

    /**
     * The application class loader.
     */
    private static class TestClassLoader extends URLClassLoader {

        public TestClassLoader() {
            super(((URLClassLoader) TestClassLoader.class.getClassLoader())
                    .getURLs(), ClassLoader.getSystemClassLoader());
        }

        // allows delegation of H2 to the AppClassLoader
        @Override
        public synchronized Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException {
            if (!name.contains(CLASS_NAME) && !name.startsWith("org.h2.")) {
                return super.loadClass(name, resolve);
            }
            Class<?> c = findLoadedClass(name);
            if (c == null) {
                try {
                    c = findClass(name);
                } catch (SecurityException e) {
                    return super.loadClass(name, resolve);
                } catch (ClassNotFoundException e) {
                    return super.loadClass(name, resolve);
                }
                if (resolve) {
                    resolveClass(c);
                }
            }
            return c;
        }
    }

}
