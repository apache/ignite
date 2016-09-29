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

package org.apache.ignite.testframework;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testsuites.IgniteIgnore;
import org.jetbrains.annotations.Nullable;
import org.junit.internal.MethodSorter;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for run junit tests.
 * Test methods marked with @Ignored annotation won't be executed.
 */
public class IgniteTestSuite extends TestSuite {
    /** Ignore default flag thread local. */
    private static final ThreadLocal<Boolean> IGNORE_DFLT = new ThreadLocal<Boolean>() {
        @Override protected Boolean initialValue() {
            return false;
        }
    };

    /** Whether to execute only ignored tests. */
    private boolean ignoredOnly;

    /**
     * Constructor.
     *
     * @param name Name.
     */
    public IgniteTestSuite(String name) {
        this(null, name);
    }

    /**
     * Constructor.
     *
     * @param theClass TestCase class
     */
    public IgniteTestSuite(Class<? extends TestCase> theClass) {
        this(theClass, ignoreDefault());
    }

    /**
     * Constructor.
     *
     * @param theClass TestCase class
     * @param ignoredOnly Whether to execute only ignored tests.
     */
    public IgniteTestSuite(Class<? extends TestCase> theClass, boolean ignoredOnly) {
        this(theClass, null, ignoredOnly);
    }

    /**
     * Constructor.
     *
     * @param theClass TestCase class
     * @param name Test suite name.
     */
    public IgniteTestSuite(Class<? extends TestCase> theClass, String name) {
        this(theClass, name, ignoreDefault());
    }

    /**
     * Constructor.
     *
     * @param theClass TestCase class
     * @param name Test suite name.
     * @param ignoredOnly Whether to execute only ignored tests.
     */
    public IgniteTestSuite(@Nullable Class<? extends TestCase> theClass, @Nullable String name, boolean ignoredOnly) {
        this.ignoredOnly = ignoredOnly;

        if (theClass != null)
            addTestsFromTestCase(theClass);

        if (name != null)
            setName(name);
    }

    /**
     * Adds a test to the suite.
     */
    @Override public void addTest(Test test) {
        super.addTest(test);
    }

    /** {@inheritDoc} */
    @Override public void addTestSuite(Class<? extends TestCase> testClass) {
        addTest(new IgniteTestSuite(testClass, ignoredOnly));
    }

    /**
     *
     * @param theClass TestCase class
     */
    private void addTestsFromTestCase(Class<?> theClass) {
        setName(theClass.getName());

        try {
            getTestConstructor(theClass);
        }
        catch (NoSuchMethodException ex) {
            addTest(warning("Class " + theClass.getName() +
                " has no public constructor TestCase(String name) or TestCase()"));

            return;
        }

        if(!Modifier.isPublic(theClass.getModifiers()))
            addTest(warning("Class " + theClass.getName() + " is not public"));
        else {
            Class superCls = theClass;

            int testAdded = 0;
            int testIgnored = 0;

            for(List<String> names = new ArrayList<>(); Test.class.isAssignableFrom(superCls);
                superCls = superCls.getSuperclass()) {

                Method[] methods = MethodSorter.getDeclaredMethods(superCls);

                for (Method each : methods) {
                    if (addTestMethod(each, names, theClass))
                        testAdded++;
                    else
                        testIgnored++;
                }
            }

            if(testAdded == 0 && testIgnored == 0)
                addTest(warning("No tests found in " + theClass.getName()));
        }
    }

    /**
     * Add test method.
     *
     * @param m Test method.
     * @param names Test name list.
     * @param theClass Test class.
     * @return Whether test method was added.
     */
    private boolean addTestMethod(Method m, List<String> names, Class<?> theClass) {
        String name = m.getName();

        if (names.contains(name))
            return false;

        if (!isPublicTestMethod(m)) {
            if (isTestMethod(m))
                addTest(warning("Test method isn't public: " + m.getName() + "(" + theClass.getCanonicalName() + ")"));

            return false;
        }

        names.add(name);

        boolean hasIgnore = m.isAnnotationPresent(IgniteIgnore.class);

        if (ignoredOnly) {
            if (hasIgnore) {
                IgniteIgnore ignore = m.getAnnotation(IgniteIgnore.class);

                String reason = ignore.value();

                if (F.isEmpty(reason))
                    throw new IllegalArgumentException("Reason is not set for ignored test [class=" +
                        theClass.getName() + ", method=" + name + ']');

                Test test = createTest(theClass, name);

                if (ignore.forceFailure()) {
                    if (test instanceof GridAbstractTest)
                        ((GridAbstractTest)test).forceFailure(ignore.value());
                    else
                        test = new ForcedFailure(name, ignore.value());
                }

                addTest(test);

                return true;
            }
        }
        else {
            if (!hasIgnore) {
                addTest(createTest(theClass, name));

                return true;
            }
        }

        return false;
    }

    /**
     * Check whether this is a test method.
     *
     * @param m Method.
     * @return {@code True} if this is a test method.
     */
    private static boolean isTestMethod(Method m) {
        return m.getParameterTypes().length == 0 &&
            m.getName().startsWith("test") &&
            m.getReturnType().equals(Void.TYPE);
    }

    /**
     * Check whether this is a public test method.
     *
     * @param m Method.
     * @return {@code True} if this is a public test method.
     */
    private static boolean isPublicTestMethod(Method m) {
        return isTestMethod(m) && Modifier.isPublic(m.getModifiers());
    }

    /**
     * @param val Default value of ignore flag.
     */
    public static void ignoreDefault(boolean val) {
        IGNORE_DFLT.set(val);
    }

    /**
     * @return Default value of ignore flag.
     */
    private static boolean ignoreDefault() {
        Boolean res = IGNORE_DFLT.get();

        return res != null && res;
    }

    /**
     * Test case simulating failure.
     */
    private static class ForcedFailure extends TestCase {
        /** Message. */
        private final String msg;

        /**
         * Constructor.
         *
         * @param name Name.
         * @param msg  Message.
         */
        private ForcedFailure(String name, String msg) {
            super(name);

            this.msg = msg;
        }

        /** {@inheritDoc} */
        @Override protected void runTest() {
            fail("Forced failure: " + msg + " (extend " + GridAbstractTest.class.getSimpleName() +
                " for better output).");
        }
    }
}
