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
import java.util.LinkedList;
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

    /** {@inheritDoc} */
    @Override public void addTest(Test test) {
        // Ignore empty test suites.
        if (test instanceof IgniteTestSuite) {
            IgniteTestSuite suite = (IgniteTestSuite)test;

            if (suite.testCount() == 0)
                return;
        }

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
        catch (NoSuchMethodException ignored) {
            addTest(warning("Class " + theClass.getName() +
                " has no public constructor TestCase(String name) or TestCase()"));

            return;
        }

        if(!Modifier.isPublic(theClass.getModifiers()))
            addTest(warning("Class " + theClass.getName() + " is not public"));
        else {
            IgnoreDescriptor clsIgnore = IgnoreDescriptor.forClass(theClass);

            Class superCls = theClass;

            int testAdded = 0;
            int testSkipped = 0;

            LinkedList<Test> addedTests = new LinkedList<>();

            for(List<String> names = new ArrayList<>(); Test.class.isAssignableFrom(superCls);
                superCls = superCls.getSuperclass()) {

                Method[] methods = MethodSorter.getDeclaredMethods(superCls);

                for (Method each : methods) {
                    AddResult res = addTestMethod(each, names, theClass, clsIgnore);

                    if (res.added()) {
                        testAdded++;

                        addedTests.add(res.test());
                    }
                    else
                        testSkipped++;
                }
            }

            if(testAdded == 0 && testSkipped == 0)
                addTest(warning("No tests found in " + theClass.getName()));

            // Populate tests count.
            for (Test test : addedTests) {
                if (test instanceof GridAbstractTest) {
                    GridAbstractTest test0 = (GridAbstractTest)test;

                    test0.forceTestCount(addedTests.size());
                }
            }
        }
    }

    /**
     * Add test method.
     *
     * @param m Test method.
     * @param names Test name list.
     * @param theClass Test class.
     * @param clsIgnore Class ignore descriptor (if any).
     * @return Result.
     */
    private AddResult addTestMethod(Method m, List<String> names, Class<?> theClass,
        @Nullable IgnoreDescriptor clsIgnore) {
        String name = m.getName();

        if (names.contains(name))
            return new AddResult(false, null);

        if (!isPublicTestMethod(m)) {
            if (isTestMethod(m))
                addTest(warning("Test method isn't public: " + m.getName() + "(" + theClass.getCanonicalName() + ")"));

            return new AddResult(false, null);
        }

        names.add(name);

        IgnoreDescriptor ignore = IgnoreDescriptor.forMethod(theClass, m);

        if (ignore == null)
            ignore = clsIgnore;

        if (ignoredOnly) {
            if (ignore != null) {
                Test test = createTest(theClass, name);

                if (ignore.forceFailure()) {
                    if (test instanceof GridAbstractTest)
                        ((GridAbstractTest)test).forceFailure(ignore.reason());
                    else
                        test = new ForcedFailure(name, ignore.reason());
                }

                addTest(test);

                return new AddResult(true, test);
            }
        }
        else {
            if (ignore == null) {
                Test test = createTest(theClass, name);

                addTest(test);

                return new AddResult(true, test);
            }
        }

        return new AddResult(false, null);
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
     * Ignore descriptor.
     */
    private static class IgnoreDescriptor {
        /** Reason. */
        private final String reason;

        /** Force failure. */
        private final boolean forceFailure;

        /**
         * Get descriptor for class (if any).
         *
         * @param cls Class.
         * @return Descriptor or {@code null}.
         */
        @Nullable public static IgnoreDescriptor forClass(Class cls) {
            Class cls0 = cls;

            while (Test.class.isAssignableFrom(cls0)) {
                if (cls0.isAnnotationPresent(IgniteIgnore.class)) {
                    IgniteIgnore ignore = (IgniteIgnore)cls0.getAnnotation(IgniteIgnore.class);

                    String reason = ignore.value();

                    if (F.isEmpty(reason))
                        throw new IllegalArgumentException("Reason is not set for ignored test [class=" +
                            cls0.getName() + ']');

                    return new IgnoreDescriptor(reason, ignore.forceFailure());
                }

                cls0 = cls0.getSuperclass();
            }

            return null;
        }

        /**
         * Get descriptor for method (if any).
         *
         * @param cls Class.
         * @param mthd Method.
         * @return Descriptor or {@code null}.
         */
        @Nullable public static IgnoreDescriptor forMethod(Class cls, Method mthd) {
            if (mthd.isAnnotationPresent(IgniteIgnore.class)) {
                IgniteIgnore ignore = mthd.getAnnotation(IgniteIgnore.class);

                String reason = ignore.value();

                if (F.isEmpty(reason))
                    throw new IllegalArgumentException("Reason is not set for ignored test [class=" +
                        cls.getName() + ", method=" + mthd.getName() + ']');

                return new IgnoreDescriptor(reason, ignore.forceFailure());
            }
            else
                return null;
        }

        /**
         * Constructor.
         *
         * @param reason Reason.
         * @param forceFailure Force failure.
         */
        private IgnoreDescriptor(String reason, boolean forceFailure) {
            this.reason = reason;
            this.forceFailure = forceFailure;
        }

        /**
         * @return Reason.
         */
        public String reason() {
            return reason;
        }

        /**
         * @return Force failure.
         */
        public boolean forceFailure() {
            return forceFailure;
        }
    }

    /**
     * Test add result.
     */
    private static class AddResult {
        /** Result. */
        private final boolean added;

        /** Test */
        private final Test test;

        /**
         * Constructor.
         *
         * @param added Result.
         * @param test Test.
         */
        public AddResult(boolean added, Test test) {
            this.added = added;
            this.test = test;
        }

        /**
         * @return Result.
         */
        public boolean added() {
            return added;
        }

        /**
         * @return Test.
         */
        public Test test() {
            return test;
        }
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
