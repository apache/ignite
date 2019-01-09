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

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.junit.internal.MethodSorter;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Base class for special test suites with ignored tests for Binary mode.
 *
 * TODO IGNITE-10777 rework this and respective subclasses for JUnit 4.
 */
class IgniteSpecialBinaryTestSuite extends TestSuite {
    /**
     * Constructor.
     *
     * @param theCls TestCase class
     */
    private IgniteSpecialBinaryTestSuite(Class<? extends TestCase> theCls) {
        this(theCls, null);
    }

    /**
     * Constructor.
     *
     * @param theCls TestCase class
     * @param name Test suite name.
     */
    IgniteSpecialBinaryTestSuite(Class<? extends TestCase> theCls, String name) {
        if (theCls != null)
            addTestsFromTestCase(theCls);

        if (name != null)
            setName(name);
    }

    /** {@inheritDoc} */
    @Override public void addTest(Test test) {
        // Ignore empty test suites.
        if (test instanceof IgniteSpecialBinaryTestSuite) {
            IgniteSpecialBinaryTestSuite suite = (IgniteSpecialBinaryTestSuite)test;

            if (suite.testCount() == 0)
                return;
        }

        super.addTest(test);
    }

    /** {@inheritDoc} */
    @Override public void addTestSuite(Class<? extends TestCase> testCls) {
        addTest(new IgniteSpecialBinaryTestSuite(testCls));
    }

    /**
     *
     * @param theCls TestCase class
     */
    private void addTestsFromTestCase(Class<?> theCls) {
        setName(theCls.getName());

        try {
            getTestConstructor(theCls);
        }
        catch (NoSuchMethodException ignored) {
            addTest(warning("Class " + theCls.getName() +
                " has no public constructor TestCase(String name) or TestCase()"));

            return;
        }

        if(!Modifier.isPublic(theCls.getModifiers()))
            addTest(warning("Class " + theCls.getName() + " is not public"));
        else {
            Class superCls = theCls;

            int testAdded = 0;
            int testSkipped = 0;

            LinkedList<Test> addedTests = new LinkedList<>();

            for(List<String> names = new ArrayList<>(); Test.class.isAssignableFrom(superCls);
                superCls = superCls.getSuperclass()) {

                Method[] methods = MethodSorter.getDeclaredMethods(superCls);

                for (Method each : methods) {
                    AddResult res = addTestMethod(each, names, theCls);

                    if (res.added()) {
                        testAdded++;

                        addedTests.add(res.test());
                    }
                    else
                        testSkipped++;
                }
            }

            if(testAdded == 0 && testSkipped == 0)
                addTest(warning("No tests found in " + theCls.getName()));

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
     * @param theCls Test class.
     * @return Result.
     */
    private AddResult addTestMethod(Method m, List<String> names, Class<?> theCls) {
        String name = m.getName();

        if (names.contains(name))
            return new AddResult(false, null);

        if (!isPublicTestMethod(m)) {
            if (isTestMethod(m))
                addTest(warning("Test method isn't public: " + m.getName() + "(" + theCls.getCanonicalName() + ")"));

            return new AddResult(false, null);
        }

        names.add(name);

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
}
