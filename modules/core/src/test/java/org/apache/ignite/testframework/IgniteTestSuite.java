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
    /** Whether to execute only ignored tests. */
    private final boolean ignoredOnly;

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
        this(theClass, false);
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
        this(theClass, name, false);
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
    @Override public void addTestSuite(Class<? extends TestCase> testClass) {
        addTestSuite(testClass, false);
    }

    /**
     * Add test class to the suite.
     *
     * @param testClass Test class.
     * @param ignoredOnly Ignore only flag.
     */
    public void addTestSuite(Class<? extends TestCase> testClass, boolean ignoredOnly) {
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

            for(List<String> names = new ArrayList<>(); Test.class.isAssignableFrom(superCls);
                superCls = superCls.getSuperclass()) {
                Method[] methods = MethodSorter.getDeclaredMethods(superCls);

                for (Method each : methods) {
                    if (addTestMethod(each, names, theClass))
                        testAdded++;
                }
            }

            if(testAdded == 0)
                addTest(warning("No tests found in " + theClass.getName()));
        }
    }

    /**
     * @param method test method
     * @param names test name list
     * @param theClass test class
     */
    private boolean addTestMethod(Method method, List<String> names, Class<?> theClass) {
        String name = method.getName();

        if(!names.contains(name) && canAddMethod(method)) {
            if(!Modifier.isPublic(method.getModifiers()))
                addTest(warning("Test method isn\'t public: " + method.getName() + "(" +
                    theClass.getCanonicalName() + ")"));
            else {
                names.add(name);

                addTest(createTest(theClass, name));

                return true;
            }
        }
        return false;
    }

    /**
     * Check whether method should be ignored.
     *
     * @param method Method.
     * @return {@code True} if it should be ignored.
     */
    protected boolean canAddMethod(Method method) {
        boolean res = method.getParameterTypes().length == 0 && method.getName().startsWith("test")
            && method.getReturnType().equals(Void.TYPE);

        if (res) {
            // If method signature and name matches check if it is ignored or not.
            boolean hasIgnore = method.isAnnotationPresent(IgniteIgnore.class);

            res = hasIgnore == ignoredOnly;
        }

        return res;
    }
}
