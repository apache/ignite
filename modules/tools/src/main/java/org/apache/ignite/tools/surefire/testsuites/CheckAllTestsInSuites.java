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

package org.apache.ignite.tools.surefire.testsuites;

import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.Request;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

/** Checks that all test classes are part of any suite. */
public class CheckAllTestsInSuites {
    /** List of test classes. Input of the test. */
    static Iterable<Class<?>> testClasses;

    /** */
    @Test
    public void check() {
        Set<Class<?>> suitedTestClasses = new HashSet<>();
        Set<Class<?>> allTestClasses = new HashSet<>();
        Set<Class<?>> suites = new HashSet<>();

        // Workaround to handle cases when a class has descenders and it's OK to skip the base class.
        // Also it works for DynamicSuite that can use a base class to create new test classes with reflection.
        Set<Class<?>> superClasses = new HashSet<>();

        for (Class<?> clazz : testClasses) {
            if (Modifier.isAbstract(clazz.getModifiers()))
                continue;

            if (clazz.getAnnotation(Ignore.class) != null)
                continue;

            Description desc = Request.aClass(clazz).getRunner().getDescription();

            if (isTestClass(desc)) {
                allTestClasses.add(clazz);
                superClasses.add(clazz.getSuperclass());
            }
            else
                processSuite(desc, suitedTestClasses, suites, superClasses);
        }

        allTestClasses.removeAll(suitedTestClasses);
        allTestClasses.removeAll(superClasses);

        if (!allTestClasses.isEmpty()) {
            StringBuilder builder = new StringBuilder("All test classes must be include in any test suite.")
                .append("\nList of non-suited classes (")
                .append(allTestClasses.size())
                .append(" items):\n");

            for (Class<?> c: allTestClasses)
                builder.append("\t").append(c.getName()).append("\n");

            throw new AssertionError(builder.toString());
        }
    }

    /**
     * Recursively hadnle suites - mark all test classes as suited.
     */
    private void processSuite(Description suite, Set<Class<?>> suitedClasses,
                              Set<Class<?>> suites, Set<Class<?>> superClasses) {
        suites.add(suite.getTestClass());

        for (Description desc: suite.getChildren()) {
            if (!isTestClass(desc))
                processSuite(desc, suitedClasses, suites, superClasses);
            else {
                suitedClasses.add(desc.getTestClass());
                superClasses.add(desc.getTestClass().getSuperclass());
            }
        }
    }

    /**
     * Check whether class is a test class or a suite.
     *
     * Suite classes are marked with RunWith annotation and value of it is a descender of Suite.class.
     * For scala tests suite must be inherited from {@code org.scalatest.Suites} class.
     * Exclusion of the rule is Parameterized.class, so classes are marked with it are test classes.
     */
    private boolean isTestClass(Description desc) {
        RunWith runWith = desc.getAnnotation(RunWith.class);

        return runWith == null
            || runWith.value().equals(Parameterized.class)
            || !(Suite.class.isAssignableFrom(runWith.value())
                || "org.scalatest.Suites".equals(desc.getTestClass().getSuperclass().getName()));
    }
}
