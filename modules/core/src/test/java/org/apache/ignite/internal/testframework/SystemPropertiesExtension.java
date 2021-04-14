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

package org.apache.ignite.internal.testframework;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit rule that manages usage of {@link WithSystemProperty} annotations.<br/>
 * Should be used in {@link ExtendWith}.
 *
 * @see WithSystemProperty
 * @see ExtendWith
 */
public class SystemPropertiesExtension implements
    BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {
    /** Class properties. */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    private List<Prop<String, String>> testMethodSysProps;

    /** Class properties. */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    private List<Prop<String, String>> testClassSysProps;

    /** {@inheritDoc} */
    @Override public void beforeAll(ExtensionContext ctx) {
        testClassSysProps = extractSystemPropertiesBeforeClass(ctx.getRequiredTestClass());
    }

    /** {@inheritDoc} */
    @Override public void afterAll(ExtensionContext context) {
        clearSystemProperties(testClassSysProps);

        testClassSysProps = null;
    }

    /** {@inheritDoc} */
    @Override public void beforeEach(ExtensionContext ctx) {
        testMethodSysProps = extractSystemPropertiesBeforeTestMethod(ctx.getRequiredTestMethod());
    }

    /** {@inheritDoc} */
    @Override public void afterEach(ExtensionContext context) {
        clearSystemProperties(testMethodSysProps);

        testMethodSysProps = null;
    }

    /**
     * Set system properties before class.
     *
     * @param testCls Current test class.
     * @return List of updated properties in reversed order.
     */
    private List<Prop<String, String>> extractSystemPropertiesBeforeClass(Class<?> testCls) {
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
        final List<Prop<String, String>> clsSysProps = new ArrayList<>();

        for (WithSystemProperty[] props : allProps) {
            for (WithSystemProperty prop : props) {
                String oldVal = System.setProperty(prop.key(), prop.value());

                clsSysProps.add(new Prop<>(prop.key(), oldVal));
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
    public List<Prop<String, String>> extractSystemPropertiesBeforeTestMethod(Method testMtd) {
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
        List<Prop<String, String>> testSysProps = new ArrayList<>();

        if (allProps != null) {
            for (WithSystemProperty prop : allProps) {
                String oldVal = System.setProperty(prop.key(), prop.value());

                testSysProps.add(new Prop<>(prop.key(), oldVal));
            }
        }

        Collections.reverse(testSysProps);

        return testSysProps;
    }

    /**
     * Return old values of updated properties.
     *
     * @param sysProps List of properties to clear.
     */
    private void clearSystemProperties(List<Prop<String, String>> sysProps) {
        if (sysProps == null)
            return; // Nothing to do.

        for (Prop<String, String> prop : sysProps) {
            if (prop.value() == null)
                System.clearProperty(prop.key());
            else
                System.setProperty(prop.key(), prop.value());
        }
    }

    /**
     * Property.
     */
    public static class Prop<K, V> {
        /** Property key. */
        private final K key;

        /** Property value. */
        private final V val;

        /**
         * @param key Property key.
         * @param val Property value.
         */
        Prop(K key, V val) {
            this.key = key;
            this.val = val;
        }

        /**
         * @return Property key.
         */
        K key() {
            return key;
        }

        /**
         * @return Property value.
         */
        V value() {
            return val;
        }
    }
}
