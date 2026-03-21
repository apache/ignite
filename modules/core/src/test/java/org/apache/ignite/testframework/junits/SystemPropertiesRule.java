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

package org.apache.ignite.testframework.junits;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

/**
 * JUnit 5 extension that manages usage of {@link WithSystemProperty} annotations.<br/>
 * Can be used at both class and method level.
 *
 * @see WithSystemProperty
 */
public class SystemPropertiesRule implements BeforeAllCallback, AfterAllCallback,
        BeforeEachCallback, AfterEachCallback, TestInstancePostProcessor,
        TestExecutionExceptionHandler, TestTemplateInvocationContextProvider {

    private static final Namespace NAMESPACE = Namespace.create(SystemPropertiesRule.class);
    private static final String CLASS_PROPERTIES_KEY = "classProperties";
    private static final String METHOD_PROPERTIES_KEY = "methodProperties";

    @Override
    public void beforeAll(ExtensionContext context) {
        Class<?> testClass = context.getRequiredTestClass();
        List<T2<String, String>> classProperties = setSystemPropertiesBeforeClass(testClass);
        getStore(context).put(CLASS_PROPERTIES_KEY, classProperties);
    }

    @Override
    public void afterAll(ExtensionContext context) {
        List<T2<String, String>> classProperties = getStore(context).get(CLASS_PROPERTIES_KEY, List.class);
        if (classProperties != null) {
            clearSystemProperties(classProperties);
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        Method testMethod = context.getRequiredTestMethod();
        List<T2<String, String>> methodProperties = setSystemPropertiesBeforeTestMethod(testMethod);
        getStore(context).put(METHOD_PROPERTIES_KEY, methodProperties);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        List<T2<String, String>> methodProperties = getStore(context).get(METHOD_PROPERTIES_KEY, List.class);
        if (methodProperties != null) {
            clearSystemProperties(methodProperties);
        }
    }

    /**
     * Searches for public method with no parameter by the class and methods name.
     *
     * @param testCls Class to search method in.
     * @param testName Method name.
     * @return Non-null method object.
     * @throws NoSuchMethodError If test method wasn't found.
     */
    private Method getTestMethod(Class<?> testCls, String testName) {
        // Remove custom parameters from "@Parameterized" test.
        int bracketIdx = testName.indexOf('[');

        String testMtdName = bracketIdx >= 0 ? testName.substring(0, bracketIdx) : testName;

        Method testMtd;
        try {
            testMtd = testCls.getMethod(testMtdName);
        }
        catch (NoSuchMethodException e) {
            throw new NoSuchMethodError(S.toString("Test method wasn't found",
                    "testClass", testCls.getSimpleName(), false,
                    "methodName", testName, false,
                    "testMtdName", testMtdName, false
            ));
        }
        return testMtd;
    }

    /**
     * Set system properties before class.
     *
     * @param testCls Current test class.
     * @return List of updated properties in reversed order.
     */
    private List<T2<String, String>> setSystemPropertiesBeforeClass(Class<?> testCls) {
        List<WithSystemProperty[]> allProps = new ArrayList<>();

        for (Class<?> cls = testCls; cls != null; cls = cls.getSuperclass()) {
            SystemPropertiesList clsProps = cls.getAnnotation(SystemPropertiesList.class);

            if (clsProps != null)
                allProps.add(clsProps.value());
            else {
                WithSystemProperty clsProp = cls.getAnnotation(WithSystemProperty.class);

                if (clsProp != null)
                    allProps.add(new WithSystemProperty[] {clsProp});
            }
        }

        Collections.reverse(allProps);

        // List of system properties to set when all tests in class are finished.
        final List<T2<String, String>> clsSysProps = new ArrayList<>();

        for (WithSystemProperty[] props : allProps) {
            for (WithSystemProperty prop : props) {
                String oldVal = System.setProperty(prop.key(), prop.value());

                clsSysProps.add(new T2<>(prop.key(), oldVal));
            }
        }

        Collections.reverse(clsSysProps);

        return clsSysProps;
    }

    /**
     * Set system properties before test method.
     *
     * @param testMtd Current test method.
     * @return List of updated properties in reversed order.
     */
    private List<T2<String, String>> setSystemPropertiesBeforeTestMethod(Method testMtd) {
        WithSystemProperty[] allProps = null;

        SystemPropertiesList testProps = testMtd.getAnnotation(SystemPropertiesList.class);

        if (testProps != null)
            allProps = testProps.value();
        else {
            WithSystemProperty testProp = testMtd.getAnnotation(WithSystemProperty.class);

            if (testProp != null)
                allProps = new WithSystemProperty[] {testProp};
        }

        // List of system properties to set when test is finished.
        List<T2<String, String>> testSysProps = new ArrayList<>();

        if (allProps != null) {
            for (WithSystemProperty prop : allProps) {
                String oldVal = System.setProperty(prop.key(), prop.value());

                testSysProps.add(new T2<>(prop.key(), oldVal));
            }
        }

        Collections.reverse(testSysProps);

        return testSysProps;
    }

    /**
     * Return old values of updated properties.
     *
     * @param sysProps List previously returned by {@link #setSystemPropertiesBeforeClass(java.lang.Class)}
     *      or {@link #setSystemPropertiesBeforeTestMethod(Method)}.
     */
    private void clearSystemProperties(List<T2<String, String>> sysProps) {
        for (T2<String, String> t2 : sysProps) {
            if (t2.getValue() == null)
                System.clearProperty(t2.getKey());
            else
                System.setProperty(t2.getKey(), t2.getValue());
        }
    }

    /**
     * Get the store for the extension context.
     */
    private Store getStore(ExtensionContext context) {
        return context.getStore(NAMESPACE);
    }

    // Required interface methods that need to be implemented but aren't used
    @Override
    public void postProcessTestInstance(Object testInstance, ExtensionContext context) throws Exception {
        // No-op - required by TestInstancePostProcessor
    }

    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        throw throwable; // Re-throw by default
    }

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return false; // Not supporting test templates
    }

    @Override
    public java.util.stream.Stream<org.junit.jupiter.api.extension.TestTemplateInvocationContext>
    provideTestTemplateInvocationContexts(ExtensionContext context) {
        return java.util.stream.Stream.empty(); // No test template contexts
    }

/*    @ExtendWith(SystemPropertiesExtension.class)
    public class YourTest {
        // test methods
    }*/
}