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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestTemplate;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

/**
 * Checks that all test classes are part of any suite.
 *
 * There are 2 inputs for this check:
 * 1. All test classes for current maven module found by surefire plugin.
 * 2. Orphaned tests by previous maven modules.
 *
 * This check never fails. It found orphaned tests for current maven module, aggregate them with tests from
 * previous modules, then persist aggregated list to a file {@link OrphanedTestCollection}.
 * After checking all modules the final list of orphaned tests is checked by {@link AssertOnOrphanedTests} job.
 */
public class CheckAllTestsInSuites {
    /**
     * List of test classes that is an input for this check. {@link IgniteTestsProvider} prepares it.
     */
    static Iterable<Class<?>> testClasses;

    /** */
    @Test
    public void check() {
        Set<String> suitedTestClasses = new HashSet<>();
        Set<String> allTestClasses = new HashSet<>();
        Set<String> suites = new HashSet<>();

        // Workaround to handle cases when a class has descenders and it's OK to skip the base class.
        // Also, it works for DynamicSuite that can use a base class to create new test classes with reflection.
        Set<String> superClasses = new HashSet<>();

        for (Class<?> clazz : testClasses) {
            if (Modifier.isAbstract(clazz.getModifiers()))
                continue;

            if (clazz.getAnnotation(Disabled.class) != null)
                continue;

            if (isTestClass(clazz)) {
                allTestClasses.add(clazz.getName());
                superClasses.add(clazz.getSuperclass().getName());
            }
            else
                processSuite(clazz, suitedTestClasses, suites, superClasses);
        }

        allTestClasses.removeAll(suitedTestClasses);
        allTestClasses.removeAll(superClasses);

        OrphanedTestCollection orphaned = new OrphanedTestCollection();

        try {
            Set<String> orphanedTests = orphaned.getOrphanedTests();

            orphanedTests.removeAll(suitedTestClasses);

            orphanedTests.addAll(allTestClasses);

            orphaned.persistOrphanedTests(orphanedTests, false);

        }
        catch (Exception e) {
            throw new RuntimeException("Failed to check orphaned tests.", e);
        }
    }

    /**
     * Recursively handle suites - mark all test classes as suited.
     */
    private void processSuite(
            Class<?> clazz,
            Set<String> suitedClasses,
            Set<String> suites,
            Set<String> superClasses
    ) {
        LauncherDiscoveryRequest request =
                LauncherDiscoveryRequestBuilder.request()
                        .selectors(selectClass(clazz))
                        .build();

/*        request.

        suites.add(suite.getTestClass().getName());

        for (Description desc: suite.getChildren()) {
            if (!isTestClass(desc))
                processSuite(desc, suitedClasses, suites, superClasses);
            else {
                suitedClasses.add(desc.getTestClass().getName());
                superClasses.add(desc.getTestClass().getSuperclass().getName());
            }
        }*/
    }

    /**
     * Check whether class is a test class or a suite.
     */
    private boolean isTestClass(Class<?> clazz) {
        boolean hasTestInstanceAnnotation = AnnotationSupport.isAnnotated(clazz, org.junit.jupiter.api.TestInstance.class);

        // Check if any method in the class is a test method
        boolean hasTestMethod = Arrays.stream(clazz.getDeclaredMethods())
                .anyMatch(CheckAllTestsInSuites::isJUnit5TestMethod);

        return hasTestInstanceAnnotation
                || hasTestMethod
                || "org.scalatest.Suites".equals(clazz.getSuperclass().getName());
    }

    public static boolean isJUnit5TestMethod(Method method) {
        return AnnotationSupport.isAnnotated(method, Test.class) ||
                AnnotationSupport.isAnnotated(method, TestFactory.class) ||
                AnnotationSupport.isAnnotated(method, TestTemplate.class);
    }
}
